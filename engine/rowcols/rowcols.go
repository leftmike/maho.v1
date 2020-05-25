package rowcols

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/btree"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/mideng"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
)

var (
	errTransactionComplete = errors.New("rowcols: transaction already completed")
)

type rowColsStore struct {
	dataDir     string
	mutex       sync.Mutex
	wal         *WAL
	tree        *btree.BTree
	ver         uint64
	commitMutex sync.Mutex
}

type transaction struct {
	rcst  *rowColsStore
	tree  *btree.BTree
	ver   uint64
	delta *btree.BTree
}

type tableDef struct {
	tn          sql.TableName
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	primary     []engine.ColumnKey
	mid         int64

	reverse uint32
	rowCols []int
}

type table struct {
	rcst *rowColsStore
	tx   *transaction
	td   *tableDef
}

type rowItem struct {
	mid        int64
	ver        uint64
	reverse    uint32
	numKeyCols uint8
	deleted    bool
	row        []sql.Value
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
}

func NewEngine(dataDir string) (engine.Engine, error) {
	os.MkdirAll(dataDir, 0755)
	f, err := os.OpenFile(filepath.Join(dataDir, "mahorowcols.wal"), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	rcst := &rowColsStore{
		dataDir: dataDir,
		wal:     &WAL{f: f},
		tree:    btree.New(16),
	}
	init, err := rcst.wal.ReadWAL(rcst)
	if err != nil {
		return nil, err
	}

	me, err := mideng.NewEngine("rowcols", rcst, init)
	if err != nil {
		return nil, err
	}

	ve := virtual.NewEngine(me)
	return ve, nil
}

func (td *tableDef) Table(ctx context.Context, tx engine.Transaction) (engine.Table, error) {
	etx := tx.(*transaction)
	return &table{
		rcst: etx.rcst,
		tx:   etx,
		td:   td,
	}, nil

}

func (td *tableDef) Columns() []sql.Identifier {
	return td.columns
}

func (td *tableDef) ColumnTypes() []sql.ColumnType {
	return td.columnTypes
}

func (td *tableDef) PrimaryKey() []engine.ColumnKey {
	return td.primary
}

func (_ *rowColsStore) MakeTableDef(tn sql.TableName, mid int64, cols []sql.Identifier,
	colTypes []sql.ColumnType, primary []engine.ColumnKey) (mideng.TableDef, error) {

	if len(primary) == 0 {
		panic(fmt.Sprintf("rowcols: table %s: missing required primary key", tn))
	}
	if len(primary) > 32 {
		panic(fmt.Sprintf("rowcols: table %s: primary key with too many columns", tn))
	}

	td := tableDef{
		tn:          tn,
		columns:     cols,
		columnTypes: colTypes,
		primary:     primary,
		mid:         mid,
	}

	td.reverse = 0
	td.rowCols = make([]int, len(cols))
	vn := len(primary)
	for cn := range cols {
		isValue := true

		for kn, ck := range primary {
			if ck.Number() == cn {
				td.rowCols[kn] = cn
				if ck.Reverse() {
					td.reverse |= 1 << kn
				}
				isValue = false
				break
			}
		}

		if isValue {
			td.rowCols[vn] = cn
			vn += 1
		}
	}

	return &td, nil
}

func (rcst *rowColsStore) Begin(sesid uint64) engine.Transaction {
	rcst.mutex.Lock()
	defer rcst.mutex.Unlock()

	return &transaction{
		rcst: rcst,
		tree: rcst.tree.Clone(),
		ver:  rcst.ver,
	}
}

func (rcst *rowColsStore) RowItem(ri rowItem) error {
	if ri.ver > rcst.ver {
		rcst.ver = ri.ver
	}
	rcst.tree.ReplaceOrInsert(ri)
	return nil
}

func (rcst *rowColsStore) commit(ctx context.Context, txVer uint64, delta *btree.BTree) error {
	rcst.commitMutex.Lock()
	defer rcst.commitMutex.Unlock()

	rcst.mutex.Lock()
	tree := rcst.tree.Clone()
	rcst.mutex.Unlock()

	ver := rcst.ver + 1
	buf := EncodeUint32([]byte{commitRecordType}, 0) // Reserve space for length.
	buf = EncodeUint64(buf, ver)

	var err error
	delta.Ascend(
		func(item btree.Item) bool {
			txri := item.(rowItem)
			cur := tree.Get(txri)
			if cur == nil {
				if !txri.deleted {
					txri.ver = ver
					tree.ReplaceOrInsert(txri)
					buf = encodeRowItem(buf, txri)
				}
			} else {
				ri := cur.(rowItem)
				if ri.ver > txVer {
					err = errors.New("rowcols: write conflict committing transaction")
					return false
				}
				if !txri.deleted || !ri.deleted {
					txri.ver = ver
					tree.ReplaceOrInsert(txri)
					buf = encodeRowItem(buf, txri)
				}
			}
			return true
		})
	if err != nil {
		return err
	}

	if err := rcst.wal.writeCommit(buf); err != nil {
		return err
	}

	rcst.mutex.Lock()
	rcst.tree = tree
	rcst.ver = ver
	rcst.mutex.Unlock()

	return nil
}

func (rctx *transaction) Commit(ctx context.Context) error {
	if rctx.rcst == nil {
		return errTransactionComplete
	}

	var err error
	if rctx.delta != nil {
		err = rctx.rcst.commit(ctx, rctx.ver, rctx.delta)
	}

	rctx.rcst = nil
	rctx.tree = nil
	rctx.delta = nil
	return err
}

func (rctx *transaction) Rollback() error {
	if rctx.rcst == nil {
		return errTransactionComplete
	}

	rctx.rcst = nil
	rctx.tree = nil
	rctx.delta = nil
	return nil
}

func (_ *transaction) NextStmt() {}

func (rctx *transaction) forWrite() {
	if rctx.delta == nil {
		rctx.delta = btree.New(16)
	}
}

func (td *tableDef) toItem(row []sql.Value, ver uint64, deleted bool) btree.Item {
	ri := rowItem{
		mid:        td.mid,
		ver:        ver,
		reverse:    td.reverse,
		deleted:    deleted,
		numKeyCols: uint8(len(td.primary)),
	}

	if row != nil {
		ri.row = make([]sql.Value, len(td.columns))
		for rdx := range td.rowCols {
			ri.row[rdx] = row[td.rowCols[rdx]]
		}
	}

	return ri
}

func (td *tableDef) toRow(ri rowItem) []sql.Value {
	if ri.row == nil {
		return nil
	}
	row := make([]sql.Value, len(td.columns))
	for rdx := range td.rowCols {
		row[td.rowCols[rdx]] = ri.row[rdx]
	}
	return row
}

func (ri rowItem) compare(ri2 rowItem) int {
	if ri.mid < ri2.mid {
		return -1
	} else if ri.mid > ri2.mid {
		return 1
	} else if ri2.row == nil {
		if ri.row == nil {
			return 0
		}
		return -1
	} else if ri.row == nil {
		return -1
	}

	for kdx := uint8(0); kdx < ri.numKeyCols; kdx += 1 {
		cmp := sql.Compare(ri.row[kdx], ri2.row[kdx])
		if cmp == 0 {
			continue
		}
		if ri.reverse&(1<<kdx) != 0 {
			return -1 * cmp
		} else {
			return cmp
		}
	}

	return 0
}

func (ri rowItem) Less(item btree.Item) bool {
	return ri.compare(item.(rowItem)) < 0
}

func (bt *table) Columns(ctx context.Context) []sql.Identifier {
	return bt.td.columns
}

func (bt *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return bt.td.columnTypes
}

func (bt *table) PrimaryKey(ctx context.Context) []engine.ColumnKey {
	return bt.td.primary
}

func (bt *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (engine.Rows, error) {
	br := &rows{
		tbl: bt,
		idx: 0,
	}

	var maxItem btree.Item
	if maxRow != nil {
		maxItem = bt.td.toItem(maxRow, 0, false)
	}

	if bt.tx.delta == nil {
		bt.tx.tree.AscendGreaterOrEqual(bt.td.toItem(minRow, 0, false),
			func(item btree.Item) bool {
				if maxItem != nil && maxItem.Less(item) {
					return false
				}
				ri := item.(rowItem)
				if ri.mid != bt.td.mid {
					return false
				}
				if !ri.deleted {
					br.rows = append(br.rows, bt.td.toRow(ri))
				}
				return true
			})
		return br, nil
	}

	var deltaRows []rowItem
	bt.tx.delta.AscendGreaterOrEqual(bt.td.toItem(minRow, 0, false),
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.mid != bt.td.mid {
				return false
			}
			deltaRows = append(deltaRows, ri)
			return true
		})

	bt.tx.tree.AscendGreaterOrEqual(bt.td.toItem(minRow, 0, false),
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.mid != bt.td.mid {
				return false
			}

			for len(deltaRows) > 0 {
				cmp := ri.compare(deltaRows[0])
				if cmp < 0 {
					break
				} else if cmp > 0 {
					if !deltaRows[0].deleted {
						br.rows = append(br.rows, bt.td.toRow(deltaRows[0]))
					}
					deltaRows = deltaRows[1:]
				} else {
					if !deltaRows[0].deleted {
						// Must be an update.
						br.rows = append(br.rows, bt.td.toRow(deltaRows[0]))
						deltaRows = deltaRows[1:]
					}
					return true
				}
			}

			if !ri.deleted {
				br.rows = append(br.rows, bt.td.toRow(ri))
			}
			return true
		})

	for _, ri := range deltaRows {
		if !ri.deleted {
			br.rows = append(br.rows, bt.td.toRow(ri))
		}
	}

	return br, nil
}

func (bt *table) Insert(ctx context.Context, row []sql.Value) error {
	bt.tx.forWrite()

	ri := bt.td.toItem(row, 0, false)
	if item := bt.tx.delta.Get(ri); item != nil {
		if !(item.(rowItem)).deleted {
			return fmt.Errorf("rowcols: %s: existing row with duplicate primary key", bt.td.tn)
		}
	} else if item := bt.tx.tree.Get(ri); item != nil && !(item.(rowItem)).deleted {
		return fmt.Errorf("rowcols: %s: existing row with duplicate primary key", bt.td.tn)
	}

	bt.tx.delta.ReplaceOrInsert(ri)
	return nil
}

func (br *rows) Columns() []sql.Identifier {
	return br.tbl.td.columns
}

func (br *rows) Close() error {
	br.tbl = nil
	br.rows = nil
	br.idx = 0
	return nil
}

func (br *rows) Next(ctx context.Context, dest []sql.Value) error {
	if br.idx == len(br.rows) {
		return io.EOF
	}

	copy(dest, br.rows[br.idx])
	br.idx += 1
	return nil
}

func (br *rows) Delete(ctx context.Context) error {
	br.tbl.tx.forWrite()

	if br.idx == 0 {
		panic(fmt.Sprintf("rowcols: table %s no row to delete", br.tbl.td.tn))
	}

	br.tbl.tx.delta.ReplaceOrInsert(br.tbl.td.toItem(br.rows[br.idx-1], 0, true))
	return nil
}

func (br *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	br.tbl.tx.forWrite()

	if br.idx == 0 {
		panic(fmt.Sprintf("rowcols: table %s no row to update", br.tbl.td.tn))
	}

	var primaryUpdated bool
	for _, update := range updates {
		for _, ck := range br.tbl.td.primary {
			if ck.Number() == update.Index {
				primaryUpdated = true
			}
		}
	}

	if primaryUpdated {
		br.Delete(ctx)

		for _, update := range updates {
			br.rows[br.idx-1][update.Index] = update.Value
		}

		return br.tbl.Insert(ctx, br.rows[br.idx-1])
	}

	for _, update := range updates {
		br.rows[br.idx-1][update.Index] = update.Value
	}
	br.tbl.tx.delta.ReplaceOrInsert(br.tbl.td.toItem(br.rows[br.idx-1], 0, false))
	return nil
}
