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

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/encode"
	"github.com/leftmike/maho/storage/mideng"
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

type tableDef struct {
	tn          sql.TableName
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	primary     []sql.ColumnKey
	mid         int64
}

type transaction struct {
	st    *keyValStore
	ver   uint64
	delta *btree.BTree
}

type table struct {
	st *keyValStore
	tx *transaction
	td *tableDef
}

type rowItem struct {
	key []byte
	row []sql.Value
}

type rows struct {
	tbl       *table
	minKey    []byte
	maxKey    []byte
	deltaRows []rowItem
	it        Iterator
	itRow     []sql.Value
	itKey     []byte
	curRow    []sql.Value
}

func NewBadgerStore(dataDir string) (storage.Store, error) {
	kv, err := MakeBadgerKV(dataDir)
	if err != nil {
		return nil, err
	}

	return newStore(kv)
}

func NewBBoltStore(dataDir string) (storage.Store, error) {
	kv, err := MakeBBoltKV(dataDir)
	if err != nil {
		return nil, err
	}

	return newStore(kv)
}

func newStore(kv KV) (storage.Store, error) {
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
	return mideng.NewStore("keyval", kvst, init)
}

func (td *tableDef) Table(ctx context.Context, tx storage.Transaction) (storage.Table, error) {
	etx := tx.(*transaction)
	return &table{
		st: etx.st,
		tx: etx,
		td: td,
	}, nil
}

func (td *tableDef) Columns() []sql.Identifier {
	return td.columns
}

func (td *tableDef) ColumnTypes() []sql.ColumnType {
	return td.columnTypes
}

func (td *tableDef) PrimaryKey() []sql.ColumnKey {
	return td.primary
}

func (kvst *keyValStore) MakeTableDef(tn sql.TableName, mid int64, cols []sql.Identifier,
	colTypes []sql.ColumnType, primary []sql.ColumnKey) (mideng.TableDef, error) {

	if len(primary) == 0 {
		panic(fmt.Sprintf("keyval: table %s: missing required primary key", tn))
	}

	td := tableDef{
		tn:          tn,
		columns:     cols,
		columnTypes: colTypes,
		primary:     primary,
		mid:         mid,
	}
	return &td, nil
}

func (kvst *keyValStore) Begin(sesid uint64) mideng.Transaction {
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
		err = upd.Set(versionKey, encode.EncodeUint64(make([]byte, 0, 8), ver))
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

func (kvtx *transaction) Changes(cfn func(mid int64, key string, row []sql.Value) bool) {
	if kvtx.delta == nil {
		return
	}

	kvtx.delta.Ascend(
		func(item btree.Item) bool {
			ri := item.(rowItem)
			var key string
			var mid int64
			if len(ri.key) < 8 {
				key = fmt.Sprintf("%v", ri.key)
			} else {
				mid = int64(binary.BigEndian.Uint64(ri.key))
				key = fmt.Sprintf("%v", ri.key[8:])
			}
			return cfn(mid, key, ri.row)
		})
}

func (kvtx *transaction) forWrite() {
	if kvtx.delta == nil {
		kvtx.delta = btree.New(16)
	}
}

func (td *tableDef) makeKey(row []sql.Value) []byte {
	buf := encode.EncodeUint64(make([]byte, 0, 8), uint64(td.mid))
	if row != nil {
		buf = append(buf, encode.MakeKey(td.primary, row)...)
	}
	return buf
}

func (td *tableDef) toItem(row []sql.Value, deleted bool) rowItem {
	ri := rowItem{
		key: td.makeKey(row),
	}
	if row != nil && !deleted {
		ri.row = append(make([]sql.Value, 0, len(td.columns)), row...)
	}
	return ri
}

func (ri rowItem) Less(item btree.Item) bool {
	return bytes.Compare(ri.key, (item.(rowItem)).key) < 0
}

func (kvt *table) Columns(ctx context.Context) []sql.Identifier {
	return kvt.td.columns
}

func (kvt *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return kvt.td.columnTypes
}

func (kvt *table) PrimaryKey(ctx context.Context) []sql.ColumnKey {
	return kvt.td.primary
}

func (kvt *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (sql.Rows, error) {
	minKey := kvt.td.makeKey(minRow)
	it, err := kvt.st.kv.Iterate(kvt.tx.ver, minKey)
	if err != nil {
		return nil, err
	}

	kvr := &rows{
		tbl:    kvt,
		minKey: minKey,
		it:     it,
	}

	if maxRow != nil {
		kvr.maxKey = kvt.td.makeKey(maxRow)
	}

	if kvt.tx.delta != nil {
		kvt.tx.delta.AscendGreaterOrEqual(kvt.td.toItem(minRow, false),
			func(item btree.Item) bool {
				ri := item.(rowItem)
				if kvr.maxKey == nil {
					if len(ri.key) < 8 {
						panic(fmt.Sprintf("keyval: key too short: %v", ri.key))
					}
					if !bytes.Equal(minKey[:8], ri.key[:8]) {
						return false
					}
				} else if bytes.Compare(kvr.maxKey, ri.key) < 0 {
					return false
				}
				kvr.deltaRows = append(kvr.deltaRows, ri)
				return true
			})
	}

	return kvr, nil
}

func (kvt *table) Insert(ctx context.Context, row []sql.Value) error {
	kvt.tx.forWrite()

	ri := kvt.td.toItem(row, false)
	if item := kvt.tx.delta.Get(ri); item != nil {
		if (item.(rowItem)).row != nil {
			return fmt.Errorf("keyval: %s: existing row with duplicate primary key", kvt.td.tn)
		}
	} else {
		err := kvt.st.kv.GetAt(kvt.tx.ver, ri.key,
			func(val []byte, ver uint64) error {
				if len(val) > 0 {
					return fmt.Errorf("keyval: %s: existing row with duplicate primary key",
						kvt.td.tn)
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

func (kvr *rows) Columns() []sql.Identifier {
	return kvr.tbl.td.columns
}

func (kvr *rows) Close() error {
	kvr.tbl = nil
	kvr.deltaRows = nil
	if kvr.it != nil {
		kvr.it.Close()
		kvr.it = nil
	}
	kvr.curRow = nil
	return nil
}

func (kvr *rows) Next(ctx context.Context, dest []sql.Value) error {
	kvr.curRow = nil
	for kvr.curRow == nil {
		if kvr.it != nil {
			for kvr.itRow == nil {
				err := kvr.it.Item(
					func(key, val []byte, ver uint64) error {
						if kvr.maxKey == nil {
							if len(key) < 8 {
								return fmt.Errorf("keyval: key too short: %v", key)
							}
							if !bytes.Equal(kvr.minKey[:8], key[:8]) {
								return io.EOF
							}
						} else if bytes.Compare(kvr.maxKey, key) < 0 {
							return io.EOF
						}

						if len(val) > 0 {
							row := encode.DecodeRowValue(val)
							if row == nil {
								return fmt.Errorf("keyval: unable to decode row: %v", val)
							}
							kvr.itRow = row
							kvr.itKey = append(make([]byte, 0, len(key)), key...)
						}

						return nil
					})

				if err == io.EOF {
					kvr.it.Close()
					kvr.it = nil
					break
				} else if err != nil {
					return err
				}
			}
		}

		if len(kvr.deltaRows) > 0 {
			if kvr.itRow != nil {
				cmp := bytes.Compare(kvr.itKey, kvr.deltaRows[0].key)
				if cmp < 0 {
					kvr.curRow = kvr.itRow
					kvr.itRow = nil
					kvr.itKey = nil
				} else if cmp > 0 {
					if kvr.deltaRows[0].row != nil {
						kvr.curRow = kvr.deltaRows[0].row
					}
					kvr.deltaRows = kvr.deltaRows[1:]
				} else {
					if kvr.deltaRows[0].row != nil {
						// Must be an update.
						kvr.curRow = kvr.deltaRows[0].row
					}
					kvr.deltaRows = kvr.deltaRows[1:]
					kvr.itRow = nil
					kvr.itKey = nil
				}
			} else {
				if kvr.deltaRows[0].row != nil {
					kvr.curRow = kvr.deltaRows[0].row
				}
				kvr.deltaRows = kvr.deltaRows[1:]
			}
		} else if kvr.itRow != nil {
			kvr.curRow = kvr.itRow
			kvr.itRow = nil
			kvr.itKey = nil
		} else {
			return io.EOF
		}
	}

	copy(dest, kvr.curRow)
	return nil
}

func (kvr *rows) Delete(ctx context.Context) error {
	kvr.tbl.tx.forWrite()

	if kvr.curRow == nil {
		panic(fmt.Sprintf("keyval: table %s no row to delete", kvr.tbl.td.tn))
	}

	kvr.tbl.tx.delta.ReplaceOrInsert(kvr.tbl.td.toItem(kvr.curRow, true))
	return nil
}

func (kvr *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	kvr.tbl.tx.forWrite()

	if kvr.curRow == nil {
		panic(fmt.Sprintf("keyval: table %s no row to update", kvr.tbl.td.tn))
	}

	var primaryUpdated bool
	for _, update := range updates {
		for _, ck := range kvr.tbl.td.primary {
			if ck.Number() == update.Index {
				primaryUpdated = true
			}
		}
	}

	updateRow := append(make([]sql.Value, 0, len(kvr.curRow)), kvr.curRow...)
	for _, update := range updates {
		updateRow[update.Index] = update.Value
	}

	if primaryUpdated {
		kvr.Delete(ctx)
		return kvr.tbl.Insert(ctx, updateRow)
	}

	kvr.tbl.tx.delta.ReplaceOrInsert(kvr.tbl.td.toItem(updateRow, false))
	return nil
}
