package kvrows

import (
	"context"
	"errors"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

var (
	notImplemented = errors.New("kvrows: not implemented")
	ErrKeyNotFound = errors.New("kvrows: key not found")
)

type Store interface {
	Begin() (Tx, error)
}

type Tx interface {
	Map(mid uint64) (Mapper, error)
	Commit() error
	Rollback() error
}

type Mapper interface {
	// XXX: is Get really needed?
	//Get(key []byte, vf func(val []byte) error) error
	Set(key, val []byte) error
	Walk(prefix []byte) Walker
}

type Walker interface {
	Close()
	Delete() error
	Next() ([]byte, bool)
	Rewind() ([]byte, bool)
	Seek(seek []byte) ([]byte, bool)
	Value(vf func(val []byte) error) error
}

type KVRows struct {
	st Store
}

func (kv *KVRows) Init(st Store) {
	kv.st = st
}

func (kv *KVRows) CreateDatabase(dbname sql.Identifier, options engine.Options) error {
	return notImplemented
}

func (kv *KVRows) DropDatabase(dbname sql.Identifier, ifExists bool, options engine.Options) error {

	return notImplemented
}

func (kv *KVRows) CreateSchema(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) error {

	return notImplemented
}

func (kv *KVRows) DropSchema(ctx context.Context, tx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	return notImplemented
}

func (kv *KVRows) LookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	return nil, notImplemented
}

func (kv *KVRows) CreateTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	return notImplemented
}

func (kv *KVRows) DropTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	return notImplemented
}

func (_ *KVRows) CreateIndex(ctx context.Context, tx engine.Transaction, idxname sql.Identifier,
	tn sql.TableName, unique bool, keys []engine.ColumnKey,
	ifNotExists bool) error {

	return notImplemented
}

func (_ *KVRows) DropIndex(ctx context.Context, tx engine.Transaction, idxname sql.Identifier,
	tn sql.TableName, ifExists bool) error {

	return notImplemented
}

func (kv *KVRows) Begin(sesid uint64) engine.Transaction {
	return nil
}

func (kv *KVRows) ListDatabases(ctx context.Context,
	tx engine.Transaction) ([]sql.Identifier, error) {

	return nil, notImplemented
}

func (kv *KVRows) ListSchemas(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	return nil, notImplemented
}

func (kv *KVRows) ListTables(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	return nil, notImplemented
}
