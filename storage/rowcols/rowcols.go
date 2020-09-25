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

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/encode"
	"github.com/leftmike/maho/util"
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

type table struct {
	st  *rowColsStore
	tl  *storage.TableLayout
	tn  sql.TableName
	tid int64
	tx  *transaction
}

type rowItem struct {
	rid int64
	ver uint64
	key []byte
	row []sql.Value // deleted: row = nil
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
}

type indexRows struct {
	tbl  *table
	il   storage.IndexLayout
	idx  int
	rows [][]sql.Value
}

func NewStore(dataDir string) (*storage.Store, error) {
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

	return storage.NewStore("rowcols", rcst, init)
}

func (rcst *rowColsStore) Table(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	tid int64, tt *engine.TableType, tl *storage.TableLayout) (engine.Table, error) {

	if len(tt.PrimaryKey()) == 0 {
		panic(fmt.Sprintf("rowcols: table %s: missing required primary key", tn))
	}

	etx := tx.(*transaction)
	return &table{
		st:  etx.st,
		tl:  tl,
		tn:  tn,
		tid: tid,
		tx:  etx,
	}, nil
}

func (rcst *rowColsStore) Begin(sesid uint64) engine.Transaction {
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
	buf := util.EncodeUint32([]byte{commitRecordType}, 0) // Reserve space for length.
	buf = util.EncodeUint64(buf, ver)

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

func (rctx *transaction) forWrite() {
	if rctx.delta == nil {
		rctx.delta = btree.New(16)
	}
}

func (ri rowItem) compare(ri2 rowItem) int {
	if ri.rid < ri2.rid {
		return -1
	} else if ri.rid > ri2.rid {
		return 1
	} else {
		return bytes.Compare(ri.key, ri2.key)
	}
}

func (ri rowItem) Less(item btree.Item) bool {
	return ri.compare(item.(rowItem)) < 0
}

func copyRow(row []sql.Value) []sql.Value {
	return append(make([]sql.Value, 0, len(row)), row...)
}

func (rct *table) toItem(row []sql.Value, deleted bool) btree.Item {
	ri := rowItem{
		rid: (rct.tid << 16) | storage.PrimaryIID,
	}
	if row != nil {
		ri.key = encode.MakeKey(rct.tl.PrimaryKey(), row)
		if !deleted {
			ri.row = append(make([]sql.Value, 0, len(rct.tl.Columns())), row...)
		}
	}
	return ri
}

func (rct *table) toIndexItem(row []sql.Value, deleted bool, il storage.IndexLayout) btree.Item {
	ri := rowItem{
		rid: (rct.tid << 16) | il.IID,
	}
	if row != nil {
		indexRow := il.RowToIndexRow(row)
		ri.key = il.MakeKey(encode.MakeKey(il.Key, indexRow), indexRow)
		if !deleted {
			ri.row = indexRow
		}
	}
	return ri
}

func (rct *table) fetchRows(ctx context.Context, minItem, maxItem btree.Item,
	rid int64) [][]sql.Value {

	var vals [][]sql.Value
	if rct.tx.delta == nil {
		rct.tx.tree.AscendGreaterOrEqual(minItem,
			func(item btree.Item) bool {
				if maxItem != nil && maxItem.Less(item) {
					return false
				}
				ri := item.(rowItem)
				if ri.rid != rid {
					return false
				}
				if ri.row != nil {
					vals = append(vals, copyRow(ri.row))
				}
				return true
			})
		return vals
	}

	var deltaRows []rowItem
	rct.tx.delta.AscendGreaterOrEqual(minItem,
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.rid != rid {
				return false
			}
			deltaRows = append(deltaRows, ri)
			return true
		})

	rct.tx.tree.AscendGreaterOrEqual(minItem,
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.rid != rid {
				return false
			}

			for len(deltaRows) > 0 {
				cmp := ri.compare(deltaRows[0])
				if cmp < 0 {
					break
				} else if cmp > 0 {
					if deltaRows[0].row != nil {
						vals = append(vals, copyRow(deltaRows[0].row))
					}
					deltaRows = deltaRows[1:]
				} else {
					if deltaRows[0].row != nil {
						// Must be an update.
						vals = append(vals, copyRow(deltaRows[0].row))
						deltaRows = deltaRows[1:]
					}
					return true
				}
			}

			if ri.row != nil {
				vals = append(vals, copyRow(ri.row))
			}
			return true
		})

	for _, ri := range deltaRows {
		if ri.row != nil {
			vals = append(vals, copyRow(ri.row))
		}
	}

	return vals
}

func (rct *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (engine.Rows, error) {
	var maxItem btree.Item
	if maxRow != nil {
		maxItem = rct.toItem(maxRow, false)
	}

	return &rows{
		tbl: rct,
		idx: 0,
		rows: rct.fetchRows(ctx, rct.toItem(minRow, false), maxItem,
			(rct.tid<<16)|storage.PrimaryIID),
	}, nil
}

func (rct *table) IndexRows(ctx context.Context, iidx int,
	minRow, maxRow []sql.Value) (engine.IndexRows, error) {

	indexes := rct.tl.Indexes()
	if iidx >= len(indexes) {
		panic(fmt.Sprintf("rowcols: table: %s: %d indexes: out of range: %d", rct.tn, len(indexes),
			iidx))
	}

	il := indexes[iidx]

	var maxItem btree.Item
	if maxRow != nil {
		maxItem = rct.toIndexItem(maxRow, false, il)
	}

	return &indexRows{
		tbl: rct,
		il:  il,
		idx: 0,
		rows: rct.fetchRows(ctx, rct.toIndexItem(minRow, false, il), maxItem,
			(rct.tid<<16)|il.IID),
	}, nil
}

func (rct *table) insertItem(ri btree.Item, idxname sql.Identifier) error {
	if item := rct.tx.delta.Get(ri); item != nil {
		if (item.(rowItem)).row != nil {
			return fmt.Errorf("rowcols: %s: primary index: existing row with duplicate key",
				rct.tn)
		}
	} else if item := rct.tx.tree.Get(ri); item != nil && (item.(rowItem)).row != nil {
		return fmt.Errorf("rowcols: %s: primary index: existing row with duplicate key", rct.tn)
	}

	rct.tx.delta.ReplaceOrInsert(ri)
	return nil
}

func (rct *table) Insert(ctx context.Context, row []sql.Value) error {
	rct.tx.forWrite()

	err := rct.insertItem(rct.toItem(row, false), sql.PRIMARY)
	if err != nil {
		return err
	}

	for idx, il := range rct.tl.Indexes() {
		err = rct.insertItem(rct.toIndexItem(row, false, il), rct.tl.IndexName(idx))
		if err != nil {
			return err
		}
	}

	return nil
}

func (rcr *rows) Columns() []sql.Identifier {
	return rcr.tbl.tl.Columns()
}

func (rcr *rows) Close() error {
	rcr.tbl = nil
	rcr.rows = nil
	rcr.idx = 0
	return nil
}

func (rcr *rows) Next(ctx context.Context) ([]sql.Value, error) {
	if rcr.idx == len(rcr.rows) {
		return nil, io.EOF
	}

	rcr.idx += 1
	return rcr.rows[rcr.idx-1], nil
}

func (rct *table) deleteRow(ctx context.Context, row []sql.Value) {
	rct.tx.delta.ReplaceOrInsert(rct.toItem(row, true))

	for _, il := range rct.tl.Indexes() {
		rct.tx.delta.ReplaceOrInsert(rct.toIndexItem(row, true, il))
	}
}

func (rcr *rows) Delete(ctx context.Context) error {
	rcr.tbl.tx.forWrite()

	if rcr.idx == 0 {
		panic(fmt.Sprintf("rowcols: table %s no row to delete", rcr.tbl.tn))
	}

	rcr.tbl.deleteRow(ctx, rcr.rows[rcr.idx-1])
	return nil
}

func (rct *table) updateIndexes(ctx context.Context, updatedCols []int,
	row, updateRow []sql.Value) error {

	indexes, updated := rct.tl.IndexesUpdated(updatedCols)
	for idx := range indexes {
		il := indexes[idx]
		if updated[idx] {
			rct.tx.delta.ReplaceOrInsert(rct.toIndexItem(row, true, il))
			err := rct.insertItem(rct.toIndexItem(updateRow, false, il), rct.tl.IndexName(idx))
			if err != nil {
				return err
			}
		} else {
			rct.tx.delta.ReplaceOrInsert(rct.toIndexItem(updateRow, false, il))
		}
	}
	return nil
}

func (rct *table) updateRow(ctx context.Context, updatedCols []int,
	row, updateRow []sql.Value) error {

	if rct.tl.PrimaryUpdated(updatedCols) {
		rct.deleteRow(ctx, row)
		err := rct.Insert(ctx, updateRow)
		if err != nil {
			return err
		}
	} else {
		rct.tx.delta.ReplaceOrInsert(rct.toItem(updateRow, false))
	}

	return rct.updateIndexes(ctx, updatedCols, row, updateRow)
}

func (rcr *rows) Update(ctx context.Context, updatedCols []int, updateRow []sql.Value) error {
	rcr.tbl.tx.forWrite()

	if rcr.idx == 0 {
		panic(fmt.Sprintf("rowcols: table %s no row to update", rcr.tbl.tn))
	}

	return rcr.tbl.updateRow(ctx, updatedCols, rcr.rows[rcr.idx-1], updateRow)
}

func (rcir *indexRows) Close() error {
	rcir.tbl = nil
	rcir.rows = nil
	rcir.idx = 0
	return nil
}

func (rcir *indexRows) Next(ctx context.Context) ([]sql.Value, error) {
	if rcir.idx == len(rcir.rows) {
		return nil, io.EOF
	}

	rcir.idx += 1
	return rcir.rows[rcir.idx-1], nil
}

func (rcir *indexRows) Delete(ctx context.Context) error {
	rcir.tbl.tx.forWrite()

	if rcir.idx == 0 {
		panic(fmt.Sprintf("rowcols: table %s no row to delete", rcir.tbl.tn))
	}

	rcir.tbl.deleteRow(ctx, rcir.getRow())
	return nil
}

func (rcir *indexRows) Update(ctx context.Context, updatedCols []int,
	updateRow []sql.Value) error {

	rcir.tbl.tx.forWrite()

	if rcir.idx == 0 {
		panic(fmt.Sprintf("rowcols: table %s no row to update", rcir.tbl.tn))
	}

	return rcir.tbl.updateRow(ctx, updatedCols, rcir.getRow(), updateRow)
}

func (rcir *indexRows) getRow() []sql.Value {
	row := make([]sql.Value, len(rcir.tbl.tl.Columns()))
	rcir.il.IndexRowToRow(rcir.rows[rcir.idx-1], row)
	key := rcir.tbl.toItem(row, false)

	var item btree.Item
	if rcir.tbl.tx.delta != nil {
		item = rcir.tbl.tx.delta.Get(key)
	}
	if item == nil {
		item = rcir.tbl.tx.tree.Get(key)
	}
	if item == nil || (item.(rowItem)).row == nil {
		panic(fmt.Sprintf("rowcols: table %s no row to get in tree", rcir.tbl.tn))
	}

	ri := item.(rowItem)
	return ri.row
}

func (rcir *indexRows) Row(ctx context.Context) ([]sql.Value, error) {
	if rcir.idx == 0 {
		panic(fmt.Sprintf("rowcols: table %s no row to get", rcir.tbl.tn))
	}

	return rcir.getRow(), nil
}
