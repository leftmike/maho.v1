package kvrows

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

const (
	configMID    = 0
	databasesMID = 1
)

var (
	notImplemented          = errors.New("kvrows: not implemented")
	ErrKeyNotFound          = errors.New("kvrows: key not found")
	ErrValueVersionMismatch = errors.New("kvrows: value version mismatch")

	metadataKey      = Key{Key: []byte("metadata"), Type: MetadataKeyType}
	databasesPrimary = []engine.ColumnKey{engine.MakeColumnKey(0, false)}
)

type storeMetadata struct {
	Epoch   uint64
	Version uint64
}

type databaseMetadata struct {
	Active bool
}

type KVRows struct {
	mutex     sync.RWMutex
	st        Store
	epoch     uint64
	version   uint64
	databases map[sql.Identifier]databaseMetadata
}

func (kv *KVRows) readGob(mid uint64, key Key, value interface{}) (Key, error) {
	ver, val, err := kv.st.ReadValue(mid, key)
	if err == ErrKeyNotFound {
		key.Version = 0
		return key, err
	} else if err != nil {
		return Key{}, err
	}
	key.Version = ver

	dec := gob.NewDecoder(bytes.NewBuffer(val))
	return key, dec.Decode(value)
}

func (kv *KVRows) writeGob(mid uint64, key Key, value interface{}) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	if err != nil {
		return err
	}
	return kv.st.WriteValue(mid, key, key.Version+1, buf.Bytes())
}

func (kv *KVRows) loadMetadata() error {
	var md storeMetadata
	key, err := kv.readGob(configMID, metadataKey, &md)
	if err != nil && err != ErrKeyNotFound {
		return err
	}

	md.Epoch += 1
	kv.epoch = md.Epoch
	kv.version = md.Version
	return kv.writeGob(configMID, key, &md)
}

func (kv *KVRows) loadDatabases() error {
	keys, vals, err := kv.st.ListValues(databasesMID)
	if err != nil {
		return err
	}

	for idx := range keys {
		sqlKey := []sql.Value{nil}
		ok := ParseSQLKey(keys[idx].Key, databasesPrimary, sqlKey)
		if !ok {
			return fmt.Errorf("kvrows: databases: corrupt primary key: %v", keys[idx].Key)
		}
		s, ok := sqlKey[0].(sql.StringValue)
		if !ok {
			return fmt.Errorf("kvrows: databases: expected string key: %s", sqlKey[0])
		}

		var md databaseMetadata
		dec := gob.NewDecoder(bytes.NewBuffer(vals[idx]))
		err = dec.Decode(&md)
		if err != nil {
			return err
		}

		kv.databases[sql.ID(string(s))] = md
	}

	return nil
}

func (kv *KVRows) Startup(st Store) error {
	kv.st = st
	kv.databases = map[sql.Identifier]databaseMetadata{}

	err := kv.loadMetadata()
	if err != nil {
		return err
	}
	err = kv.loadDatabases()
	if err != nil {
		return err
	}
	return nil
}

func (_ *KVRows) CreateSystemTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("kvrows: use virtual engine with kvrows engine")
}

func (_ *KVRows) CreateInfoTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("kvrows: use virtual engine with kvrows engine")
}

func (_ *KVRows) IsTransactional() bool {
	return true
}

func (kv *KVRows) updateDatabase(dbname sql.Identifier, active bool) error {
	key := Key{
		Key:  MakeSQLKey([]sql.Value{sql.StringValue(dbname.String())}, databasesPrimary),
		Type: MetadataKeyType,
	}
	var md databaseMetadata
	key, err := kv.readGob(databasesMID, key, &md)
	if err != nil && err != ErrKeyNotFound {
		return err
	}

	md.Active = active
	err = kv.writeGob(databasesMID, key, &md)
	if err != nil {
		return err
	}
	kv.databases[dbname] = md
	return nil
}

func (kv *KVRows) CreateDatabase(dbname sql.Identifier, options engine.Options) error {
	if len(options) != 0 {
		return fmt.Errorf("kvrows: unexpected option to create database: %s", dbname)
	}

	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	md, ok := kv.databases[dbname]
	if ok && md.Active {
		return fmt.Errorf("kvrows: database %s already exists", dbname)
	}

	return kv.updateDatabase(dbname, true)
}

func (kv *KVRows) DropDatabase(dbname sql.Identifier, ifExists bool, options engine.Options) error {
	if len(options) != 0 {
		return fmt.Errorf("kvrows: unexpected option to drop database: %s", dbname)
	}

	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	md, ok := kv.databases[dbname]
	if !ok || !md.Active {
		if ifExists {
			return nil
		}
		return fmt.Errorf("kvrows: database %s does not exist", dbname)
	}

	return kv.updateDatabase(dbname, false)
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
