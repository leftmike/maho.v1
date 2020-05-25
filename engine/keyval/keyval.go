package keyval

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
	kvst  *keyValStore
	ver   uint64
	delta *btree.BTree
}

type table struct {
	kvst *keyValStore
	tx   *transaction
	td   *tableDef
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
}

func NewEngine(dataDir string) (engine.Engine, error) {
	kvst := &keyValStore{}

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
		kvst: etx.kvst,
		tx:   etx,
		td:   td,
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
		kvst: kvst,
		ver:  kvst.ver,
	}
}

func (kvtx *transaction) Commit(ctx context.Context) error {
	if kvtx.kvst == nil {
		return errTransactionComplete
	}

	// XXX

	kvtx.kvst = nil
	return nil
}

func (kvtx *transaction) Rollback() error {
	if kvtx.kvst == nil {
		return errTransactionComplete
	}

	// XXX

	kvtx.kvst = nil
	return nil
}

func (_ *transaction) NextStmt() {}

func (kvtx *transaction) forWrite() {
	if kvtx.delta == nil {
		kvtx.delta = btree.New(16)
	}
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
	return nil, errors.New("Rows: not implemented")
}

func (kvt *table) Insert(ctx context.Context, row []sql.Value) error {
	kvt.tx.forWrite()

	return errors.New("Insert: not implemented")
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

	return errors.New("Delete: not implemented")
}

func (kvr *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	kvr.tbl.tx.forWrite()

	if kvr.idx == 0 {
		panic(fmt.Sprintf("keyval: table %s no row to update", kvr.tbl.td.tn))
	}

	return errors.New("Update: not implemented")
}
