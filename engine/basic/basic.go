package basic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/btree"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/mideng"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
)

var (
	errTransactionComplete = errors.New("basic: transaction already completed")
)

type basicEngine struct {
	mutex sync.Mutex
	tree  *btree.BTree
}

type transaction struct {
	be   *basicEngine
	tree *btree.BTree
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
	be *basicEngine
	tx *transaction
	td *tableDef
}

type rowItem struct {
	mid        int64
	reverse    uint32
	numKeyCols uint8
	row        []sql.Value
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
}

func NewEngine(dataDir string) (engine.Engine, error) {
	be := &basicEngine{
		tree: btree.New(16),
	}
	me, err := mideng.NewEngine("basic", be, true)
	if err != nil {
		return nil, err
	}
	ve := virtual.NewEngine(me)
	return ve, nil
}

func (td *tableDef) Table(ctx context.Context, tx engine.Transaction) (engine.Table, error) {
	etx := tx.(*transaction)
	return &table{
		be: etx.be,
		tx: etx,
		td: td,
	}, nil
}

func (_ *basicEngine) MakeTableDef(tn sql.TableName, mid int64, cols []sql.Identifier,
	colTypes []sql.ColumnType, primary []engine.ColumnKey) (mideng.TableDef, error) {

	if len(primary) == 0 {
		panic(fmt.Sprintf("basic: table %s: missing required primary key", tn))
	}
	if len(primary) > 32 {
		panic(fmt.Sprintf("basic: table %s: primary key with too many columns", tn))
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

func (be *basicEngine) Begin(sesid uint64) engine.Transaction {
	be.mutex.Lock()
	return &transaction{
		be:   be,
		tree: be.tree,
	}
}

func (btx *transaction) Commit(ctx context.Context) error {
	if btx.be == nil {
		return errTransactionComplete
	}

	btx.be.tree = btx.tree
	btx.be.mutex.Unlock()
	btx.be = nil
	btx.tree = nil
	return nil
}

func (btx *transaction) Rollback() error {
	if btx.be == nil {
		return errTransactionComplete
	}

	btx.be.mutex.Unlock()
	btx.be = nil
	btx.tree = nil
	return nil
}

func (_ *transaction) NextStmt() {}

func (btx *transaction) forWrite() {
	if btx.tree == btx.be.tree {
		btx.tree = btx.be.tree.Clone()
	}
}

func (td *tableDef) toItem(row []sql.Value) btree.Item {
	ri := rowItem{
		mid:        td.mid,
		reverse:    td.reverse,
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
		panic(fmt.Sprintf("basic: table %s contains nil row", td.tn))
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
		maxItem = bt.td.toItem(maxRow)
	}

	bt.tx.tree.AscendGreaterOrEqual(bt.td.toItem(minRow),
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.mid != bt.td.mid {
				return false
			}
			br.rows = append(br.rows, bt.td.toRow(ri))
			return true
		})
	return br, nil
}

func (bt *table) Insert(ctx context.Context, row []sql.Value) error {
	bt.tx.forWrite()

	if bt.tx.tree.Has(bt.td.toItem(row)) {
		return fmt.Errorf("basic: %s: existing row with duplicate primary key", bt.td.tn)
	}

	bt.tx.tree.ReplaceOrInsert(bt.td.toItem(row))
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
		panic(fmt.Sprintf("basic: table %s no row to delete", br.tbl.td.tn))
	}

	br.tbl.tx.tree.Delete(br.tbl.td.toItem(br.rows[br.idx-1]))
	return nil
}

func (br *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	br.tbl.tx.forWrite()

	if br.idx == 0 {
		panic(fmt.Sprintf("basic: table %s no row to update", br.tbl.td.tn))
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

	row := append(make([]sql.Value, 0, len(br.rows[br.idx-1])), br.rows[br.idx-1]...)
	for _, update := range updates {
		row[update.Index] = update.Value
	}
	br.tbl.tx.tree.ReplaceOrInsert(br.tbl.td.toItem(row))
	return nil
}
