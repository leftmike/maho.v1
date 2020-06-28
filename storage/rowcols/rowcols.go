package rowcols

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/btree"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/encode"
	"github.com/leftmike/maho/storage/tblstore"
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
	st    *rowColsStore
	tree  *btree.BTree
	ver   uint64
	delta *btree.BTree
}

type tableStructure struct {
	tn          sql.TableName
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	primary     []sql.ColumnKey
	mid         int64

	reverse uint32
	rowCols []int
}

type table struct {
	st *rowColsStore
	tx *transaction
	ts *tableStructure
}

type rowItem struct {
	mid int64
	ver uint64
	key []byte
	row []sql.Value // deleted: row = nil
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
}

func NewStore(dataDir string) (storage.Store, error) {
	os.MkdirAll(dataDir, 0755)
	f, err := os.OpenFile(filepath.Join(dataDir, "maho-rowcols.wal"), os.O_RDWR|os.O_CREATE, 0755)
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

	return tblstore.NewStore("rowcols", rcst, init)
}

func (ts *tableStructure) Table(ctx context.Context, tx storage.Transaction) (storage.Table,
	error) {

	etx := tx.(*transaction)
	return &table{
		st: etx.st,
		tx: etx,
		ts: ts,
	}, nil

}

func (ts *tableStructure) Columns() []sql.Identifier {
	return ts.columns
}

func (ts *tableStructure) ColumnTypes() []sql.ColumnType {
	return ts.columnTypes
}

func (ts *tableStructure) PrimaryKey() []sql.ColumnKey {
	return ts.primary
}

func (_ *rowColsStore) MakeTableStructure(tn sql.TableName, mid int64, cols []sql.Identifier,
	colTypes []sql.ColumnType, primary []sql.ColumnKey) (tblstore.TableStructure, error) {

	if len(primary) == 0 {
		panic(fmt.Sprintf("rowcols: table %s: missing required primary key", tn))
	}
	if len(primary) > 32 {
		panic(fmt.Sprintf("rowcols: table %s: primary key with too many columns", tn))
	}

	ts := tableStructure{
		tn:          tn,
		columns:     cols,
		columnTypes: colTypes,
		primary:     primary,
		mid:         mid,
	}

	ts.reverse = 0
	ts.rowCols = make([]int, len(cols))
	vn := len(primary)
	for cn := range cols {
		isValue := true

		for kn, ck := range primary {
			if ck.Number() == cn {
				ts.rowCols[kn] = cn
				if ck.Reverse() {
					ts.reverse |= 1 << kn
				}
				isValue = false
				break
			}
		}

		if isValue {
			ts.rowCols[vn] = cn
			vn += 1
		}
	}

	return &ts, nil
}

func (rcst *rowColsStore) Begin(sesid uint64) tblstore.Transaction {
	rcst.mutex.Lock()
	defer rcst.mutex.Unlock()

	return &transaction{
		st:   rcst,
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
	buf := encode.EncodeUint32([]byte{commitRecordType}, 0) // Reserve space for length.
	buf = encode.EncodeUint64(buf, ver)

	var err error
	delta.Ascend(
		func(item btree.Item) bool {
			txri := item.(rowItem)
			cur := tree.Get(txri)
			if cur == nil {
				if txri.row != nil {
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
				if txri.row != nil || ri.row != nil {
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
	if rctx.st == nil {
		return errTransactionComplete
	}

	var err error
	if rctx.delta != nil {
		err = rctx.st.commit(ctx, rctx.ver, rctx.delta)
	}

	rctx.st = nil
	rctx.tree = nil
	rctx.delta = nil
	return err
}

func (rctx *transaction) Rollback() error {
	if rctx.st == nil {
		return errTransactionComplete
	}

	rctx.st = nil
	rctx.tree = nil
	rctx.delta = nil
	return nil
}

func (_ *transaction) NextStmt() {}

func (rctx *transaction) Changes(cfn func(mid int64, key string, row []sql.Value) bool) {
	if rctx.delta == nil {
		return
	}

	rctx.delta.Ascend(
		func(item btree.Item) bool {
			ri := item.(rowItem)
			return cfn(ri.mid, fmt.Sprintf("%v: %d", ri.key, ri.ver), ri.row)
		})
}

func (rctx *transaction) forWrite() {
	if rctx.delta == nil {
		rctx.delta = btree.New(16)
	}
}

func (ts *tableStructure) toItem(row []sql.Value, deleted bool) btree.Item {
	ri := rowItem{
		mid: ts.mid,
	}
	if row != nil {
		ri.key = encode.MakeKey(ts.primary, row)
		if !deleted {
			ri.row = append(make([]sql.Value, 0, len(ts.columns)), row...)
		}
	}
	return ri
}

func (ri rowItem) compare(ri2 rowItem) int {
	if ri.mid < ri2.mid {
		return -1
	} else if ri.mid > ri2.mid {
		return 1
	} else {
		return bytes.Compare(ri.key, ri2.key)
	}
}

func (ri rowItem) Less(item btree.Item) bool {
	return ri.compare(item.(rowItem)) < 0
}

func (rct *table) Columns(ctx context.Context) []sql.Identifier {
	return rct.ts.columns
}

func (rct *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return rct.ts.columnTypes
}

func (rct *table) PrimaryKey(ctx context.Context) []sql.ColumnKey {
	return rct.ts.primary
}

func copyRow(row []sql.Value) []sql.Value {
	return append(make([]sql.Value, 0, len(row)), row...)
}

func (rct *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (sql.Rows, error) {
	rcr := &rows{
		tbl: rct,
		idx: 0,
	}

	var maxItem btree.Item
	if maxRow != nil {
		maxItem = rct.ts.toItem(maxRow, false)
	}

	if rct.tx.delta == nil {
		rct.tx.tree.AscendGreaterOrEqual(rct.ts.toItem(minRow, false),
			func(item btree.Item) bool {
				if maxItem != nil && maxItem.Less(item) {
					return false
				}
				ri := item.(rowItem)
				if ri.mid != rct.ts.mid {
					return false
				}
				if ri.row != nil {
					rcr.rows = append(rcr.rows, copyRow(ri.row))
				}
				return true
			})
		return rcr, nil
	}

	var deltaRows []rowItem
	rct.tx.delta.AscendGreaterOrEqual(rct.ts.toItem(minRow, false),
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.mid != rct.ts.mid {
				return false
			}
			deltaRows = append(deltaRows, ri)
			return true
		})

	rct.tx.tree.AscendGreaterOrEqual(rct.ts.toItem(minRow, false),
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.mid != rct.ts.mid {
				return false
			}

			for len(deltaRows) > 0 {
				cmp := ri.compare(deltaRows[0])
				if cmp < 0 {
					break
				} else if cmp > 0 {
					if deltaRows[0].row != nil {
						rcr.rows = append(rcr.rows, copyRow(deltaRows[0].row))
					}
					deltaRows = deltaRows[1:]
				} else {
					if deltaRows[0].row != nil {
						// Must be an update.
						rcr.rows = append(rcr.rows, copyRow(deltaRows[0].row))
						deltaRows = deltaRows[1:]
					}
					return true
				}
			}

			if ri.row != nil {
				rcr.rows = append(rcr.rows, copyRow(ri.row))
			}
			return true
		})

	for _, ri := range deltaRows {
		if ri.row != nil {
			rcr.rows = append(rcr.rows, copyRow(ri.row))
		}
	}

	return rcr, nil
}

func (rct *table) Insert(ctx context.Context, row []sql.Value) error {
	rct.tx.forWrite()

	ri := rct.ts.toItem(row, false)
	if item := rct.tx.delta.Get(ri); item != nil {
		if (item.(rowItem)).row != nil {
			return fmt.Errorf("rowcols: %s: existing row with duplicate primary key", rct.ts.tn)
		}
	} else if item := rct.tx.tree.Get(ri); item != nil && (item.(rowItem)).row != nil {
		return fmt.Errorf("rowcols: %s: existing row with duplicate primary key", rct.ts.tn)
	}

	rct.tx.delta.ReplaceOrInsert(ri)
	return nil
}

func (rcr *rows) Columns() []sql.Identifier {
	return rcr.tbl.ts.columns
}

func (rcr *rows) Close() error {
	rcr.tbl = nil
	rcr.rows = nil
	rcr.idx = 0
	return nil
}

func (rcr *rows) Next(ctx context.Context, dest []sql.Value) error {
	if rcr.idx == len(rcr.rows) {
		return io.EOF
	}

	copy(dest, rcr.rows[rcr.idx])
	rcr.idx += 1
	return nil
}

func (rcr *rows) Delete(ctx context.Context) error {
	rcr.tbl.tx.forWrite()

	if rcr.idx == 0 {
		panic(fmt.Sprintf("rowcols: table %s no row to delete", rcr.tbl.ts.tn))
	}

	rcr.tbl.tx.delta.ReplaceOrInsert(rcr.tbl.ts.toItem(rcr.rows[rcr.idx-1], true))
	return nil
}

func (rcr *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	rcr.tbl.tx.forWrite()

	if rcr.idx == 0 {
		panic(fmt.Sprintf("rowcols: table %s no row to update", rcr.tbl.ts.tn))
	}

	var primaryUpdated bool
	for _, update := range updates {
		for _, ck := range rcr.tbl.ts.primary {
			if ck.Number() == update.Index {
				primaryUpdated = true
			}
		}
	}

	if primaryUpdated {
		rcr.Delete(ctx)

		for _, update := range updates {
			rcr.rows[rcr.idx-1][update.Index] = update.Value
		}

		return rcr.tbl.Insert(ctx, rcr.rows[rcr.idx-1])
	}

	for _, update := range updates {
		rcr.rows[rcr.idx-1][update.Index] = update.Value
	}
	rcr.tbl.tx.delta.ReplaceOrInsert(rcr.tbl.ts.toItem(rcr.rows[rcr.idx-1], false))
	return nil
}
