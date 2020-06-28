package basic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/btree"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/encode"
	"github.com/leftmike/maho/storage/tblstore"
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

type tableStruct struct {
	mid         int64
	tn          sql.TableName
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	primary     []sql.ColumnKey
}

type table struct {
	bst *basicStore
	tx  *transaction
	ts  *tableStruct
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

func NewStore(dataDir string) (storage.Store, error) {
	bst := &basicStore{
		tree: btree.New(16),
	}
	return tblstore.NewStore("basic", bst, true)
}

func (ts *tableStruct) Table(ctx context.Context, tx storage.Transaction) (storage.Table,
	error) {

	etx := tx.(*transaction)
	return &table{
		bst: etx.bst,
		tx:  etx,
		ts:  ts,
	}, nil
}

func (ts *tableStruct) Columns() []sql.Identifier {
	return ts.columns
}

func (ts *tableStruct) ColumnTypes() []sql.ColumnType {
	return ts.columnTypes
}

func (ts *tableStruct) PrimaryKey() []sql.ColumnKey {
	return ts.primary
}

func (_ *basicStore) MakeTableStruct(tn sql.TableName, mid int64, cols []sql.Identifier,
	colTypes []sql.ColumnType, primary []sql.ColumnKey) (tblstore.TableStruct, error) {

	if len(primary) == 0 {
		panic(fmt.Sprintf("basic: table %s: missing required primary key", tn))
	}

	return &tableStruct{
		mid:         mid,
		tn:          tn,
		columns:     cols,
		columnTypes: colTypes,
		primary:     primary,
	}, nil
}

func (bst *basicStore) Begin(sesid uint64) tblstore.Transaction {
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

func (btx *transaction) Changes(cfn func(mid int64, key string, row []sql.Value) bool) {
	// XXX
}

func (btx *transaction) forWrite() {
	if btx.tree == btx.bst.tree {
		btx.tree = btx.bst.tree.Clone()
	}
}

func (ts *tableStruct) toItem(row []sql.Value) btree.Item {
	ri := rowItem{
		mid: ts.mid,
	}
	if row != nil {
		ri.key = encode.MakeKey(ts.primary, row)
		ri.row = append(make([]sql.Value, 0, len(ts.columns)), row...)
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
	return bt.ts.columns
}

func (bt *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return bt.ts.columnTypes
}

func (bt *table) PrimaryKey(ctx context.Context) []sql.ColumnKey {
	return bt.ts.primary
}

func (bt *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (sql.Rows, error) {
	br := &rows{
		tbl: bt,
		idx: 0,
	}

	var maxItem btree.Item
	if maxRow != nil {
		maxItem = bt.ts.toItem(maxRow)
	}

	bt.tx.tree.AscendGreaterOrEqual(bt.ts.toItem(minRow),
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.mid != bt.ts.mid {
				return false
			}
			br.rows = append(br.rows, append(make([]sql.Value, 0, len(ri.row)), ri.row...))
			return true
		})
	return br, nil
}

func (bt *table) Insert(ctx context.Context, row []sql.Value) error {
	bt.tx.forWrite()

	if bt.tx.tree.Has(bt.ts.toItem(row)) {
		return fmt.Errorf("basic: %s: existing row with duplicate primary key", bt.ts.tn)
	}

	bt.tx.tree.ReplaceOrInsert(bt.ts.toItem(row))
	return nil
}

func (br *rows) Columns() []sql.Identifier {
	return br.tbl.ts.columns
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
		panic(fmt.Sprintf("basic: table %s no row to delete", br.tbl.ts.tn))
	}

	br.tbl.tx.tree.Delete(br.tbl.ts.toItem(br.rows[br.idx-1]))
	return nil
}

func (br *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	br.tbl.tx.forWrite()

	if br.idx == 0 {
		panic(fmt.Sprintf("basic: table %s no row to update", br.tbl.ts.tn))
	}

	var primaryUpdated bool
	for _, update := range updates {
		for _, ck := range br.tbl.ts.primary {
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
	br.tbl.tx.tree.ReplaceOrInsert(br.tbl.ts.toItem(row))
	return nil
}
