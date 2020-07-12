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
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/encode"
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

type table struct {
	bst     *basicStore
	tn      sql.TableName
	tid     int64
	cols    []sql.Identifier
	primary []sql.ColumnKey
	tx      *transaction
}

type rowItem struct {
	rid int64
	key []byte
	row []sql.Value
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
}

func NewStore(dataDir string) (*storage.Store, error) {
	bst := &basicStore{
		tree: btree.New(16),
	}
	return storage.NewStore("basic", bst, true)
}

func (_ *basicStore) Table(ctx context.Context, tx sql.Transaction, tn sql.TableName, tid int64,
	tt *engine.TableType, tl *storage.TableLayout) (engine.Table, error) {

	primary := tt.PrimaryKey()
	if len(primary) == 0 {
		panic(fmt.Sprintf("basic: table %s: missing required primary key", tn))
	}

	etx := tx.(*transaction)
	return &table{
		bst:     etx.bst,
		tn:      tn,
		tid:     tid,
		cols:    tt.Columns(),
		primary: primary,
		tx:      etx,
	}, nil
}

func (bst *basicStore) Begin(sesid uint64) sql.Transaction {
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

func (bt *table) toItem(row []sql.Value) btree.Item {
	ri := rowItem{
		rid: (bt.tid << 16) | storage.PrimaryIID,
	}
	if row != nil {
		ri.key = encode.MakeKey(bt.primary, row)
		ri.row = append(make([]sql.Value, 0, len(bt.cols)), row...)
	}
	return ri
}

func (ri rowItem) Less(item btree.Item) bool {
	ri2 := item.(rowItem)
	if ri.rid < ri2.rid {
		return true
	}
	return ri.rid == ri2.rid && bytes.Compare(ri.key, ri2.key) < 0
}

func (bt *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (engine.Rows, error) {
	br := &rows{
		tbl: bt,
		idx: 0,
	}

	var maxItem btree.Item
	if maxRow != nil {
		maxItem = bt.toItem(maxRow)
	}

	rid := (bt.tid << 16) | storage.PrimaryIID
	bt.tx.tree.AscendGreaterOrEqual(bt.toItem(minRow),
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.rid != rid {
				return false
			}
			br.rows = append(br.rows, append(make([]sql.Value, 0, len(ri.row)), ri.row...))
			return true
		})
	return br, nil
}

func (bt *table) Insert(ctx context.Context, row []sql.Value) error {
	bt.tx.forWrite()

	if bt.tx.tree.Has(bt.toItem(row)) {
		return fmt.Errorf("basic: %s: existing row with duplicate primary key", bt.tn)
	}

	bt.tx.tree.ReplaceOrInsert(bt.toItem(row))
	return nil
}

func (br *rows) Columns() []sql.Identifier {
	return br.tbl.cols
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
		panic(fmt.Sprintf("basic: table %s no row to delete", br.tbl.tn))
	}

	br.tbl.tx.tree.Delete(br.tbl.toItem(br.rows[br.idx-1]))
	return nil
}

func (br *rows) Update(ctx context.Context, updates []sql.ColumnUpdate,
	check func(row []sql.Value) error) error {

	br.tbl.tx.forWrite()

	if br.idx == 0 {
		panic(fmt.Sprintf("basic: table %s no row to update", br.tbl.tn))
	}

	var primaryUpdated bool
	for _, update := range updates {
		for _, ck := range br.tbl.primary {
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

		if check != nil {
			err := check(br.rows[br.idx-1])
			if err != nil {
				return err
			}
		}

		return br.tbl.Insert(ctx, br.rows[br.idx-1])
	}

	row := append(make([]sql.Value, 0, len(br.rows[br.idx-1])), br.rows[br.idx-1]...)
	for _, update := range updates {
		row[update.Index] = update.Value
	}

	if check != nil {
		err := check(row)
		if err != nil {
			return err
		}
	}

	br.tbl.tx.tree.ReplaceOrInsert(br.tbl.toItem(row))
	return nil
}
