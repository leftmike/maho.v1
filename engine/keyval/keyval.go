package keyval

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
	Update(ver uint64) Updater
}

type keyValStore struct {
	mutex sync.Mutex
	kv    KV
	ver   uint64
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

func (kvst *keyValStore) Begin(sesid uint64) engine.Transaction {
	kvst.mutex.Lock()
	defer kvst.mutex.Unlock()

	return &transaction{
		st:  kvst,
		ver: kvst.ver,
	}
}

func (kvtx *transaction) Commit(ctx context.Context) error {
	if kvtx.st == nil {
		return errTransactionComplete
	}

	// XXX

	kvtx.st = nil
	return nil
}

func (kvtx *transaction) Rollback() error {
	if kvtx.st == nil {
		return errTransactionComplete
	}

	// XXX

	kvtx.st = nil
	return nil
}

func (_ *transaction) NextStmt() {}

func (kvtx *transaction) forWrite() {
	if kvtx.delta == nil {
		kvtx.delta = btree.New(16)
	}
}

func makeKey(mid int64, primary []engine.ColumnKey, row []sql.Value) []byte {
	buf := encode.EncodeUint64(make([]byte, 0, 8), uint64(mid))
	if row != nil {
		buf = append(buf, encode.MakeKey(primary, row)...)
	}
	return buf
}

func (td *tableDef) toItem(row []sql.Value, deleted bool) rowItem {
	ri := rowItem{
		key: makeKey(td.mid, td.primary, row),
	}
	if row != nil && !deleted {
		ri.row = append(make([]sql.Value, 0, len(td.columns)), row...)
	}
	return ri
}

func toRow(ri rowItem) []sql.Value {
	if ri.row == nil {
		return nil
	}
	return append(make([]sql.Value, 0, len(ri.row)), ri.row...)
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
	kvr := &rows{
		tbl: kvt,
		idx: 0,
	}

	var maxKey []byte
	if maxRow == nil {
		maxKey = makeKey(kvt.td.mid+1, nil, nil)
	} else {
		maxKey = makeKey(kvt.td.mid, kvt.td.primary, maxRow)
	}

	if kvt.tx.delta == nil {
		err := kvt.st.kv.IterateAt(kvt.tx.ver, makeKey(kvt.td.mid, kvt.td.primary, minRow),
			func(key, val []byte, ver uint64) (bool, error) {
				// XXX: ver doesn't appear to be necessary?
				if bytes.Compare(maxKey, key) <= 0 {
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
			if bytes.Compare(maxKey, ri.key) <= 0 {
				return false
			}
			deltaRows = append(deltaRows, ri)
			return true
		})
	_ = deltaRows

	err := kvt.st.kv.IterateAt(kvt.tx.ver, makeKey(kvt.td.mid, kvt.td.primary, minRow),
		func(key, val []byte, ver uint64) (bool, error) {
			// XXX: ver doesn't appear to be necessary?
			if bytes.Compare(maxKey, key) <= 0 {
				return false, nil
			}

			for len(deltaRows) > 0 {
				cmp := bytes.Compare(key, deltaRows[0].key)
				if cmp < 0 {
					break
				} else if cmp > 0 {
					if deltaRows[0].row != nil {
						kvr.rows = append(kvr.rows, toRow(deltaRows[0]))
					}
					deltaRows = deltaRows[1:]
				} else {
					if deltaRows[0].row != nil {
						// Must be an update.
						kvr.rows = append(kvr.rows, toRow(deltaRows[0]))
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
			kvr.rows = append(kvr.rows, toRow(ri))
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
		/*
			XXX
					if item := bt.tx.tree.Get(ri); item != nil && (item.(rowItem)).row != nil {
						return fmt.Errorf("keyval: %s: existing row with duplicate primary key", bt.td.tn)
					}
		*/
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
