package kvrows

import (
	"context"
	"errors"
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

var (
	notImplemented          = errors.New("kvrows: not implemented")
	ErrKeyNotFound          = errors.New("kvrows: key not found")
	ErrValueVersionMismatch = errors.New("kvrows: value version mismatch")
)

type Store interface {
	Begin(writable bool) (Tx, error)
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

var (
	versionedPrimary = []engine.ColumnKey{engine.MakeColumnKey(0, false)}
)

type versionedTable struct {
	st  Store
	mid uint64
}

func MakeVersionedTable(st Store, mid uint64) *versionedTable {
	return &versionedTable{
		st:  st,
		mid: mid,
	}
}

// Get the value of a key; return the value version or an error.
func (vtbl *versionedTable) Get(key sql.Value, vf func(val []byte) error) (uint64, error) {
	tx, err := vtbl.st.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	m, err := tx.Map(vtbl.mid)
	if err != nil {
		return 0, err
	}

	w := m.Walk(MakePrefix([]sql.Value{key}, versionedPrimary))
	defer w.Close()
	k, ok := w.Rewind()
	if !ok {
		return 0, ErrKeyNotFound
	}

	ver, ok := ParseDurableKey(k, versionedPrimary, []sql.Value{nil})
	if !ok {
		return 0, fmt.Errorf("kvrows: unable to parse key %v", k)
	}

	err = w.Value(vf)
	if err != nil {
		return 0, err
	}

	if _, ok = w.Next(); ok {
		return 0, fmt.Errorf("kvrows: versioned table %d: multiple rows with identical key: %s",
			vtbl.mid, key)
	}
	return ver, nil
}

// Conditionally set a value: if the key does not exist, ver must be 0; otherwise, ver
// must equal the existing value.
func (vtbl *versionedTable) Set(key sql.Value, ver uint64, value []byte) error {
	tx, err := vtbl.st.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(vtbl.mid)
	if err != nil {
		return err
	}

	w := m.Walk(MakePrefix([]sql.Value{key}, versionedPrimary))
	defer w.Close()
	k, ok := w.Rewind()
	if !ok {
		if ver != 0 {
			return ErrValueVersionMismatch
		}
	} else {
		curVer, ok := ParseDurableKey(k, versionedPrimary, []sql.Value{nil})
		if !ok {
			return fmt.Errorf("kvrows: unable to parse key %v", k)
		}
		if ver != curVer {
			return ErrValueVersionMismatch
		}

		err = w.Delete()
		if err != nil {
			return err
		}
	}

	err = m.Set(MakeDurableKey([]sql.Value{key}, versionedPrimary, ver+1), value)
	if err != nil {
		return err
	}

	w.Close()
	return tx.Commit()
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
