package basic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/btree"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/encode"
	"github.com/leftmike/maho/engine/mideng"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
)

var (
	errTransactionComplete = errors.New("basic: transaction already completed")
)

type basicStore struct {
	mutex sync.Mutex
	tree  *btree.BTree
}

type transaction struct {
	bst  *basicStore
	tree *btree.BTree
}

type tableDef struct {
	mid         int64
	tn          sql.TableName
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	primary     []engine.ColumnKey
}

type table struct {
	bst *basicStore
	tx  *transaction
	td  *tableDef
}

type rowItem struct {
	mid int64
	key []byte
	row []sql.Value
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
}

func NewEngine(dataDir string) (engine.Engine, error) {
	bst := &basicStore{
		tree: btree.New(16),
	}
	me, err := mideng.NewEngine("basic", bst, true)
	if err != nil {
		return nil, err
	}
	ve := virtual.NewEngine(me)
	return ve, nil
}

func (td *tableDef) Table(ctx context.Context, tx engine.Transaction) (engine.Table, error) {
	etx := tx.(*transaction)
	return &table{
		bst: etx.bst,
		tx:  etx,
		td:  td,
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

func (_ *basicStore) MakeTableDef(tn sql.TableName, mid int64, cols []sql.Identifier,
	colTypes []sql.ColumnType, primary []engine.ColumnKey) (mideng.TableDef, error) {

	if len(primary) == 0 {
		panic(fmt.Sprintf("basic: table %s: missing required primary key", tn))
	}

	return &tableDef{
		mid:         mid,
		tn:          tn,
		columns:     cols,
		columnTypes: colTypes,
		primary:     primary,
	}, nil
}

func (bst *basicStore) Begin(sesid uint64) engine.Transaction {
	bst.mutex.Lock()
	return &transaction{
		bst:  bst,
		tree: bst.tree,
	}
}

func (btx *transaction) Commit(ctx context.Context) error {
	if btx.bst == nil {
		return errTransactionComplete
	}

	btx.bst.tree = btx.tree
	btx.bst.mutex.Unlock()
	btx.bst = nil
	btx.tree = nil
	return nil
}

func (btx *transaction) Rollback() error {
	if btx.bst == nil {
		return errTransactionComplete
	}

	btx.bst.mutex.Unlock()
	btx.bst = nil
	btx.tree = nil
	return nil
}

func (_ *transaction) NextStmt() {}

func (btx *transaction) forWrite() {
	if btx.tree == btx.bst.tree {
		btx.tree = btx.bst.tree.Clone()
	}
}

func (td *tableDef) toItem(row []sql.Value) btree.Item {
	ri := rowItem{
		mid: td.mid,
	}
	if row != nil {
		ri.key = encode.MakeKey(td.primary, row)
		ri.row = append(make([]sql.Value, 0, len(td.columns)), row...)
	}
	return ri
}

func (ri rowItem) Less(item btree.Item) bool {
	ri2 := item.(rowItem)
	if ri.mid < ri2.mid {
		return true
	}
	return ri.mid == ri2.mid && bytes.Compare(ri.key, ri2.key) < 0
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
			br.rows = append(br.rows, append(make([]sql.Value, 0, len(ri.row)), ri.row...))
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
