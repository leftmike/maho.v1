package keyval

import (
	"bytes"
	"context"
	"encoding/binary"
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
	errTransactionComplete = errors.New("keyval: transaction already completed")
)

type Updater interface {
	Get(key []byte, fn func(val []byte, ver uint64) error) error
	Set(key, val []byte) error
	CommitAt(ver uint64) error
	Rollback()
}

type KV interface {
	IterateAt(ver uint64, key []byte, fn func(key, val []byte, ver uint64) (bool, error)) error
	GetAt(ver uint64, key []byte, fn func(val []byte, ver uint64) error) error
	Update(ver uint64) Updater
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
	primary     []engine.ColumnKey
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
	tbl  *table
	idx  int
	rows [][]sql.Value
}

func NewEngine(dataDir string) (engine.Engine, error) {
	kv, err := MakeBadgerKV(dataDir)
	if err != nil {
		return nil, err
	}

	kvst := &keyValStore{
		kv: kv,
	}
	me, err := mideng.NewEngine("keyval", kvst, true)
	if err != nil {
		return nil, err
	}
	ve := virtual.NewEngine(me)

	return ve, nil
}

func (td *tableDef) Table(ctx context.Context, tx engine.Transaction) (engine.Table, error) {
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

func (td *tableDef) PrimaryKey() []engine.ColumnKey {
	return td.primary
}

func (kvst *keyValStore) MakeTableDef(tn sql.TableName, mid int64, cols []sql.Identifier,
	colTypes []sql.ColumnType, primary []engine.ColumnKey) (mideng.TableDef, error) {

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
	upd := kvst.kv.Update(kvst.ver)

	var err error
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
	if err != nil {
		upd.Rollback()
		return err
	}

	err = upd.CommitAt(ver)
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

func (kvt *table) PrimaryKey(ctx context.Context) []engine.ColumnKey {
	return kvt.td.primary
}

func (kvt *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (engine.Rows, error) {
	rows, err := kvt.makeRows(ctx, minRow, maxRow)
	if err != nil {
		return nil, err
	}

	for _, row := range rows.rows {
		if row == nil {
			fmt.Printf("row == nil %d\n%v\n", kvt.td.mid, rows.rows)
		} else if len(row) == 0 {
			fmt.Printf("len(row) == 0 %d\n%v\n", kvt.td.mid, rows.rows)
		}
	}
	return rows, nil
}

func (kvt *table) makeRows(ctx context.Context, minRow, maxRow []sql.Value) (*rows, error) {
	kvr := &rows{
		tbl: kvt,
		idx: 0,
	}

	minKey := kvt.td.makeKey(minRow)

	var maxKey []byte
	if maxRow != nil {
		maxKey = kvt.td.makeKey(maxRow)
	}

	if kvt.tx.delta == nil {
		err := kvt.st.kv.IterateAt(kvt.tx.ver, minKey,
			func(key, val []byte, ver uint64) (bool, error) {
				if maxKey == nil {
					if len(key) < 8 {
						return false, fmt.Errorf("keyval: key too short: %v", key)
					}
					if !bytes.Equal(minKey[:8], key[:8]) {
						return false, nil
					}
				} else if bytes.Compare(maxKey, key) < 0 {
					return false, nil
				}
				if len(val) > 0 {
					row := encode.DecodeRowValue(val)
					if row == nil {
						return false, fmt.Errorf("keyval: unable to decode row: %v", val)
					}
					kvr.rows = append(kvr.rows, row)
				}
				return true, nil
			})
		if err != nil {
			return nil, err
		}
		return kvr, nil
	}

	var deltaRows []rowItem
	kvt.tx.delta.AscendGreaterOrEqual(kvt.td.toItem(minRow, false),
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
			deltaRows = append(deltaRows, ri)
			return true
		})

	err := kvt.st.kv.IterateAt(kvt.tx.ver, minKey,
		func(key, val []byte, ver uint64) (bool, error) {
			if maxKey == nil {
				if len(key) < 8 {
					return false, fmt.Errorf("keyval: key too short: %v", key)
				}
				if !bytes.Equal(minKey[:8], key[:8]) {
					return false, nil
				}
			} else if bytes.Compare(maxKey, key) < 0 {
				return false, nil
			}

			for len(deltaRows) > 0 {
				cmp := bytes.Compare(key, deltaRows[0].key)
				if cmp < 0 {
					break
				} else if cmp > 0 {
					if deltaRows[0].row != nil {
						kvr.rows = append(kvr.rows,
							append(make([]sql.Value, 0, len(deltaRows[0].row)),
								deltaRows[0].row...))
					}
					deltaRows = deltaRows[1:]
				} else {
					if deltaRows[0].row != nil {
						// Must be an update.
						kvr.rows = append(kvr.rows,
							append(make([]sql.Value, 0, len(deltaRows[0].row)),
								deltaRows[0].row...))
						deltaRows = deltaRows[1:]
					}
					return true, nil
				}
			}

			if len(val) > 0 {
				row := encode.DecodeRowValue(val)
				if row == nil {
					return false, fmt.Errorf("keyval: unable to decode row: %v", val)
				}
				kvr.rows = append(kvr.rows, row)
			}
			return true, nil
		})
	if err != nil {
		return nil, err
	}

	for _, ri := range deltaRows {
		if ri.row != nil {
			kvr.rows = append(kvr.rows, append(make([]sql.Value, 0, len(ri.row)), ri.row...))
		}
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
	kvr.rows = nil
	kvr.idx = 0
	return nil
}

func (kvr *rows) Next(ctx context.Context, dest []sql.Value) error {
	if kvr.idx == len(kvr.rows) {
		return io.EOF
	}

	copy(dest, kvr.rows[kvr.idx])
	kvr.idx += 1
	return nil
}

func (kvr *rows) Delete(ctx context.Context) error {
	kvr.tbl.tx.forWrite()

	if kvr.idx == 0 {
		panic(fmt.Sprintf("keyval: table %s no row to delete", kvr.tbl.td.tn))
	}

	kvr.tbl.tx.delta.ReplaceOrInsert(kvr.tbl.td.toItem(kvr.rows[kvr.idx-1], true))
	return nil
}

func (kvr *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	kvr.tbl.tx.forWrite()

	if kvr.idx == 0 {
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

	if primaryUpdated {
		kvr.Delete(ctx)

		for _, update := range updates {
			kvr.rows[kvr.idx-1][update.Index] = update.Value
		}

		return kvr.tbl.Insert(ctx, kvr.rows[kvr.idx-1])
	}

	for _, update := range updates {
		kvr.rows[kvr.idx-1][update.Index] = update.Value
	}
	kvr.tbl.tx.delta.ReplaceOrInsert(kvr.tbl.td.toItem(kvr.rows[kvr.idx-1], false))
	return nil
}
