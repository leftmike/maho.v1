package keyval

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/google/btree"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/encode"
	"github.com/leftmike/maho/util"
)

var (
	errTransactionComplete = errors.New("keyval: transaction already completed")
	versionKey             = []byte{0, 0, 0, 0, 0, 0, 0, 0, 'v', 'e', 'r', 's', 'i', 'o', 'n'}
)

type Updater interface {
	Get(key []byte, fn func(val []byte, ver uint64) error) error
	Set(key, val []byte) error
	Commit() error
	Rollback()
}

type Iterator interface {
	Item(fn func(key, val []byte, ver uint64) error) error
	Close()
}

type KV interface {
	Iterate(ver uint64, key []byte) (Iterator, error)
	GetAt(ver uint64, key []byte, fn func(val []byte, ver uint64) error) error
	Update(ver uint64) (Updater, error)
}

type keyValStore struct {
	mutex       sync.Mutex
	kv          KV
	ver         uint64
	commitMutex sync.Mutex
}

type transaction struct {
	st    *keyValStore
	ver   uint64
	delta *btree.BTree
}

type table struct {
	st  *keyValStore
	tl  *storage.TableLayout
	tn  sql.TableName
	tid int64
	tx  *transaction
}

type rowItem struct {
	key []byte
	row []sql.Value
}

type rowsIterator struct {
	minKey    []byte
	maxKey    []byte
	deltaRows []rowItem
	it        Iterator
	itRow     []sql.Value
	itKey     []byte
	curRow    []sql.Value
}

type rows struct {
	tbl *table
	rowsIterator
}

type indexRows struct {
	tbl *table
	il  storage.IndexLayout
	rowsIterator
}

func NewBadgerStore(dataDir string) (*storage.Store, error) {
	kv, err := MakeBadgerKV(dataDir)
	if err != nil {
		return nil, err
	}

	return newStore(kv)
}

func NewBBoltStore(dataDir string) (*storage.Store, error) {
	kv, err := MakeBBoltKV(dataDir)
	if err != nil {
		return nil, err
	}

	return newStore(kv)
}

func newStore(kv KV) (*storage.Store, error) {
	var ver uint64
	err := kv.GetAt(math.MaxUint64, versionKey,
		func(val []byte, keyVer uint64) error {
			if len(val) != 8 {
				return fmt.Errorf("keyval: versionKey: len(val) != 8: %d", len(val))
			}
			ver = binary.BigEndian.Uint64(val)
			if ver != keyVer {
				return fmt.Errorf("keyval: version mismatch: %d and %d", ver, keyVer)
			}
			return nil
		})
	var init bool
	if err == io.EOF {
		init = true
	} else if err != nil {
		return nil, err
	}

	kvst := &keyValStore{
		kv:  kv,
		ver: ver,
	}
	return storage.NewStore("keyval", kvst, init)
}

func (kvst *keyValStore) Table(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	tid int64, tt *engine.TableType, tl *storage.TableLayout) (engine.Table, error) {

	if len(tt.PrimaryKey()) == 0 {
		panic(fmt.Sprintf("keyval: table %s: missing required primary key", tn))
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

func (kvst *keyValStore) Begin(sesid uint64) engine.Transaction {
	kvst.mutex.Lock()
	defer kvst.mutex.Unlock()

	return &transaction{
		st:  kvst,
		ver: kvst.ver,
	}
}

func (kvst *keyValStore) commit(ctx context.Context, txVer uint64, delta *btree.BTree) error {
	kvst.commitMutex.Lock()
	defer kvst.commitMutex.Unlock()

	ver := kvst.ver + 1
	upd, err := kvst.kv.Update(ver)
	if err != nil {
		return err
	}

	delta.Ascend(
		func(item btree.Item) bool {
			txri := item.(rowItem)
			err = upd.Get(txri.key,
				func(val []byte, ver uint64) error {
					if ver > txVer {
						return errors.New("keyval: write conflict committing transaction")
					}
					if txri.row != nil || len(val) > 0 {
						var newVal []byte
						if txri.row != nil {
							newVal = encode.EncodeRowValue(txri.row)
						}
						return upd.Set(txri.key, newVal)
					}

					return nil
				})
			if err == io.EOF {
				if txri.row != nil {
					err = upd.Set(txri.key, encode.EncodeRowValue(txri.row))
				} else {
					err = nil
				}
			}

			if err != nil {
				return false
			}
			return true
		})
	if err == nil {
		err = upd.Set(versionKey, util.EncodeUint64(make([]byte, 0, 8), ver))
	}
	if err != nil {
		upd.Rollback()
		return err
	}

	err = upd.Commit()
	if err != nil {
		return err
	}

	kvst.mutex.Lock()
	kvst.ver = ver
	kvst.mutex.Unlock()

	return nil
}

func (kvtx *transaction) Commit(ctx context.Context) error {
	if kvtx.st == nil {
		return errTransactionComplete
	}

	var err error
	if kvtx.delta != nil {
		err = kvtx.st.commit(ctx, kvtx.ver, kvtx.delta)
	}

	kvtx.st = nil
	kvtx.delta = nil
	return err
}

func (kvtx *transaction) Rollback() error {
	if kvtx.st == nil {
		return errTransactionComplete
	}

	kvtx.st = nil
	kvtx.delta = nil
	return nil
}

func (_ *transaction) NextStmt() {}

func (kvtx *transaction) forWrite() {
	if kvtx.delta == nil {
		kvtx.delta = btree.New(16)
	}
}

func (kvt *table) makeKey(key []sql.ColumnKey, iid int64, row []sql.Value) []byte {
	buf := util.EncodeUint64(make([]byte, 0, 8), uint64((kvt.tid<<16)|iid))
	if row != nil {
		buf = append(buf, encode.MakeKey(key, row)...)
	}
	return buf
}

func (kvt *table) makePrimaryKey(row []sql.Value) []byte {
	return kvt.makeKey(kvt.tl.PrimaryKey(), storage.PrimaryIID, row)
}

func (kvt *table) makeIndexKey(il storage.IndexLayout, row []sql.Value) []byte {
	return il.MakeKey(kvt.makeKey(il.Key, il.IID, row), row)
}

func (kvt *table) toItem(row []sql.Value, deleted bool) rowItem {
	ri := rowItem{
		key: kvt.makePrimaryKey(row),
	}
	if row != nil && !deleted {
		ri.row = append(make([]sql.Value, 0, len(kvt.tl.Columns())), row...)
	}
	return ri
}

func (kvt *table) toIndexItem(row []sql.Value, deleted bool, il storage.IndexLayout) rowItem {
	var indexRow []sql.Value
	if row != nil {
		indexRow = il.RowToIndexRow(row)
	}
	ri := rowItem{
		key: kvt.makeIndexKey(il, indexRow),
	}
	if !deleted {
		ri.row = indexRow
	}
	return ri
}

func (ri rowItem) Less(item btree.Item) bool {
	return bytes.Compare(ri.key, (item.(rowItem)).key) < 0
}

func (kvt *table) deltaRows(ctx context.Context, minItem btree.Item,
	minKey, maxKey []byte) []rowItem {

	var items []rowItem
	if kvt.tx.delta != nil {
		kvt.tx.delta.AscendGreaterOrEqual(minItem,
			func(item btree.Item) bool {
				ri := item.(rowItem)
				if maxKey == nil {
					if len(ri.key) < 8 {
						panic(fmt.Sprintf("keyval: key too short: %v", ri.key))
					}
					if !bytes.Equal(minKey[:8], ri.key[:8]) {
						return false
					}
				} else if bytes.Compare(maxKey, ri.key) < 0 {
					return false
				}
				items = append(items, ri)
				return true
			})
	}

	return items
}

func (kvt *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (engine.Rows, error) {
	minKey := kvt.makePrimaryKey(minRow)
	it, err := kvt.st.kv.Iterate(kvt.tx.ver, minKey)
	if err != nil {
		return nil, err
	}

	var maxKey []byte
	if maxRow != nil {
		maxKey = kvt.makePrimaryKey(maxRow)
	}

	return &rows{
		tbl: kvt,
		rowsIterator: rowsIterator{
			minKey:    minKey,
			maxKey:    maxKey,
			it:        it,
			deltaRows: kvt.deltaRows(ctx, kvt.toItem(minRow, false), minKey, maxKey),
		},
	}, nil
}

func (kvt *table) IndexRows(ctx context.Context, iidx int,
	minRow, maxRow []sql.Value) (engine.IndexRows, error) {

	indexes := kvt.tl.Indexes()
	if iidx >= len(indexes) {
		panic(fmt.Sprintf("keyval: table: %s: %d indexes: out of range: %d", kvt.tn, len(indexes),
			iidx))
	}
	il := indexes[iidx]

	var minKey []byte
	if minRow != nil {
		minKey = kvt.makeIndexKey(il, il.RowToIndexRow(minRow))
	} else {
		minKey = kvt.makeIndexKey(il, nil)
	}

	it, err := kvt.st.kv.Iterate(kvt.tx.ver, minKey)
	if err != nil {
		return nil, err
	}

	var maxKey []byte
	if maxRow != nil {
		maxKey = kvt.makeIndexKey(il, il.RowToIndexRow(maxRow))
	}

	return &indexRows{
		tbl: kvt,
		il:  il,
		rowsIterator: rowsIterator{
			minKey:    minKey,
			maxKey:    maxKey,
			it:        it,
			deltaRows: kvt.deltaRows(ctx, kvt.toIndexItem(minRow, false, il), minKey, maxKey),
		},
	}, nil
}

func (kvt *table) insert(ri rowItem, idxname sql.Identifier) error {
	if item := kvt.tx.delta.Get(ri); item != nil {
		if (item.(rowItem)).row != nil {
			return fmt.Errorf("keyval: %s: %s index: existing row with duplicate key", kvt.tn,
				idxname)
		}
	} else {
		err := kvt.st.kv.GetAt(kvt.tx.ver, ri.key,
			func(val []byte, ver uint64) error {
				if len(val) > 0 {
					return fmt.Errorf("keyval: %s: %s index: existing row with duplicate key",
						kvt.tn, idxname)
				}
				return nil
			})
		if err != nil && err != io.EOF {
			return err
		}
	}

	kvt.tx.delta.ReplaceOrInsert(ri)
	return nil
}

func (kvt *table) Insert(ctx context.Context, row []sql.Value) error {
	kvt.tx.forWrite()

	err := kvt.insert(kvt.toItem(row, false), sql.PRIMARY)
	if err != nil {
		return err
	}

	for idx, il := range kvt.tl.Indexes() {
		err = kvt.insert(kvt.toIndexItem(row, false, il), kvt.tl.IndexName(idx))
		if err != nil {
			return err
		}
	}

	return nil
}

func (kvr *rows) Columns() []sql.Identifier {
	return kvr.tbl.tl.Columns()
}

func (kvr *rows) Close() error {
	kvr.tbl = nil
	kvr.closeIterator()
	return nil
}

func (kvri *rowsIterator) closeIterator() {
	kvri.deltaRows = nil
	if kvri.it != nil {
		kvri.it.Close()
		kvri.it = nil
	}
	kvri.curRow = nil
}

func (kvri *rowsIterator) Next(ctx context.Context) ([]sql.Value, error) {
	kvri.curRow = nil
	for kvri.curRow == nil {
		if kvri.it != nil {
			for kvri.itRow == nil {
				err := kvri.it.Item(
					func(key, val []byte, ver uint64) error {
						if kvri.maxKey == nil {
							if len(key) < 8 {
								return fmt.Errorf("keyval: key too short: %v", key)
							}
							if !bytes.Equal(kvri.minKey[:8], key[:8]) {
								return io.EOF
							}
						} else if bytes.Compare(kvri.maxKey, key) < 0 {
							return io.EOF
						}

						if len(val) > 0 {
							row := encode.DecodeRowValue(val)
							if row == nil {
								return fmt.Errorf("keyval: unable to decode row: %v", val)
							}
							kvri.itRow = row
							kvri.itKey = append(make([]byte, 0, len(key)), key...)
						}

						return nil
					})

				if err == io.EOF {
					kvri.it.Close()
					kvri.it = nil
					break
				} else if err != nil {
					return nil, err
				}
			}
		}

		if len(kvri.deltaRows) > 0 {
			if kvri.itRow != nil {
				cmp := bytes.Compare(kvri.itKey, kvri.deltaRows[0].key)
				if cmp < 0 {
					kvri.curRow = kvri.itRow
					kvri.itRow = nil
					kvri.itKey = nil
				} else if cmp > 0 {
					if kvri.deltaRows[0].row != nil {
						kvri.curRow = kvri.deltaRows[0].row
					}
					kvri.deltaRows = kvri.deltaRows[1:]
				} else {
					if kvri.deltaRows[0].row != nil {
						// Must be an update.
						kvri.curRow = kvri.deltaRows[0].row
					}
					kvri.deltaRows = kvri.deltaRows[1:]
					kvri.itRow = nil
					kvri.itKey = nil
				}
			} else {
				if kvri.deltaRows[0].row != nil {
					kvri.curRow = kvri.deltaRows[0].row
				}
				kvri.deltaRows = kvri.deltaRows[1:]
			}
		} else if kvri.itRow != nil {
			kvri.curRow = kvri.itRow
			kvri.itRow = nil
			kvri.itKey = nil
		} else {
			return nil, io.EOF
		}
	}

	return kvri.curRow, nil
}

func (kvt *table) deleteRow(ctx context.Context, row []sql.Value) {
	kvt.tx.delta.ReplaceOrInsert(kvt.toItem(row, true))

	for _, il := range kvt.tl.Indexes() {
		kvt.tx.delta.ReplaceOrInsert(kvt.toIndexItem(row, true, il))
	}
}

func (kvr *rows) Delete(ctx context.Context) error {
	kvr.tbl.tx.forWrite()

	if kvr.curRow == nil {
		panic(fmt.Sprintf("keyval: table %s no row to delete", kvr.tbl.tn))
	}

	kvr.tbl.deleteRow(ctx, kvr.curRow)
	return nil
}

func (kvt *table) updateIndexes(ctx context.Context, updatedCols []int,
	row, updateRow []sql.Value) error {

	indexes, updated := kvt.tl.IndexesUpdated(updatedCols)
	for idx := range indexes {
		il := indexes[idx]
		if updated[idx] {
			kvt.tx.delta.ReplaceOrInsert(kvt.toIndexItem(row, true, il))
			err := kvt.insert(kvt.toIndexItem(updateRow, false, il), kvt.tl.IndexName(idx))
			if err != nil {
				return err
			}
		} else {
			kvt.tx.delta.ReplaceOrInsert(kvt.toIndexItem(updateRow, false, il))
		}
	}
	return nil
}

func (kvt *table) updateRow(ctx context.Context, updatedCols []int,
	row, updateRow []sql.Value) error {

	if kvt.tl.PrimaryUpdated(updatedCols) {
		kvt.deleteRow(ctx, row)
		err := kvt.Insert(ctx, updateRow)
		if err != nil {
			return err
		}
	} else {
		kvt.tx.delta.ReplaceOrInsert(kvt.toItem(updateRow, false))
	}

	return kvt.updateIndexes(ctx, updatedCols, row, updateRow)
}

func (kvr *rows) Update(ctx context.Context, updatedCols []int, updateRow []sql.Value) error {
	kvr.tbl.tx.forWrite()

	if kvr.curRow == nil {
		panic(fmt.Sprintf("keyval: table %s no row to update", kvr.tbl.tn))
	}

	return kvr.tbl.updateRow(ctx, updatedCols, kvr.curRow, updateRow)
}

func (kvir *indexRows) Close() error {
	kvir.tbl = nil
	kvir.closeIterator()
	return nil
}

func (kvir *indexRows) Delete(ctx context.Context) error {
	kvir.tbl.tx.forWrite()

	if kvir.curRow == nil {
		panic(fmt.Sprintf("keyval: table %s no row to delete", kvir.tbl.tn))
	}

	kvir.tbl.deleteRow(ctx, kvir.getRow())
	return nil
}

func (kvir *indexRows) Update(ctx context.Context, updatedCols []int,
	updateRow []sql.Value) error {

	kvir.tbl.tx.forWrite()

	if kvir.curRow == nil {
		panic(fmt.Sprintf("keyval: table %s no row to update", kvir.tbl.tn))
	}

	return kvir.tbl.updateRow(ctx, updatedCols, kvir.getRow(), updateRow)
}

func (kvir *indexRows) getRow() []sql.Value {
	row := make([]sql.Value, len(kvir.tbl.tl.Columns()))
	kvir.il.IndexRowToRow(kvir.curRow, row)

	if kvir.tbl.tx.delta != nil {
		item := kvir.tbl.tx.delta.Get(kvir.tbl.toItem(row, false))
		if item != nil {
			ri := item.(rowItem)
			if ri.row == nil {
				panic(fmt.Sprintf("keyval: table %s no row to get in tree", kvir.tbl.tn))
			}
			return ri.row
		}
	}

	var ret []sql.Value
	err := kvir.tbl.tx.st.kv.GetAt(kvir.tbl.tx.ver, kvir.tbl.makePrimaryKey(row),
		func(val []byte, ver uint64) error {
			if len(val) == 0 {
				return errors.New("missing value")
			}

			ret = encode.DecodeRowValue(val)
			if ret == nil {
				return fmt.Errorf("unable to decode row: %v", val)
			}

			return nil
		})

	if err != nil {
		panic(fmt.Sprintf("keyval: table %s: %s", kvir.tbl.tn, err))
	}
	return ret
}

func (kvir *indexRows) Row(ctx context.Context) ([]sql.Value, error) {
	if kvir.curRow == nil {
		panic(fmt.Sprintf("keyval: table %s no row to get", kvir.tbl.tn))
	}

	return kvir.getRow(), nil
}
