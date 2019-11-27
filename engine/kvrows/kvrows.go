package kvrows

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

const (
	configMID    = 0
	databasesMID = 1
	schemasMID   = 2
	tablesMID    = 3
)

var (
	notImplemented          = errors.New("kvrows: not implemented")
	errTransactionCommitted = errors.New("kvrows: transaction committed")
	errTransactionAborted   = errors.New("kvrows: transaction aborted")
	ErrKeyNotFound          = errors.New("kvrows: key not found")
	ErrValueVersionMismatch = errors.New("kvrows: value version mismatch")

	metadataKey      = Key{Key: []byte("metadata"), Type: MetadataKeyType}
	databasesPrimary = []engine.ColumnKey{engine.MakeColumnKey(0, false)}
	schemasPrimary   = []engine.ColumnKey{
		engine.MakeColumnKey(0, false),
		engine.MakeColumnKey(1, false),
	}
	tablesPrimary = []engine.ColumnKey{
		engine.MakeColumnKey(0, false),
		engine.MakeColumnKey(1, false),
		engine.MakeColumnKey(2, false),
	}
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
	lastTID   uint64
	databases map[sql.Identifier]databaseMetadata
}

type TransactionState byte

const (
	ActiveState    TransactionState = 0
	CommittedState TransactionState = 1
	AbortedState   TransactionState = 2
	UnknownState   TransactionState = 3
)

type transactionMetadata struct {
	State   TransactionState
	Epoch   uint64
	Version uint64
}

type transaction struct {
	kv    *KVRows
	key   TransactionKey
	sid   uint64
	sesid uint64
	state TransactionState
}

func (kv *KVRows) readGob(ctx context.Context, mid uint64, key Key, value interface{}) (Key,
	error) {

	ver, val, err := kv.st.ReadValue(ctx, mid, key)
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

func (kv *KVRows) writeGob(ctx context.Context, mid uint64, key Key, value interface{}) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	if err != nil {
		return err
	}
	return kv.st.WriteValue(ctx, mid, key, key.Version+1, buf.Bytes())
}

func (kv *KVRows) loadMetadata(ctx context.Context) error {
	var md storeMetadata
	key, err := kv.readGob(ctx, configMID, metadataKey, &md)
	if err != nil && err != ErrKeyNotFound {
		return err
	}

	md.Epoch += 1
	kv.epoch = md.Epoch
	kv.version = md.Version
	return kv.writeGob(ctx, configMID, key, &md)
}

func (kv *KVRows) loadDatabases(ctx context.Context) error {
	keys, vals, err := kv.st.ListValues(ctx, databasesMID)
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

	ctx := context.Background()
	err := kv.loadMetadata(ctx)
	if err != nil {
		return err
	}
	err = kv.loadDatabases(ctx)
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

func makeDatabaseKey(dbname sql.Identifier) []byte {
	return MakeSQLKey([]sql.Value{sql.StringValue(dbname.String())}, databasesPrimary)
}

func (kv *KVRows) updateDatabase(ctx context.Context, dbname sql.Identifier, active bool) error {
	key := Key{
		Key:  makeDatabaseKey(dbname),
		Type: MetadataKeyType,
	}
	var md databaseMetadata
	key, err := kv.readGob(ctx, databasesMID, key, &md)
	if err != nil && err != ErrKeyNotFound {
		return err
	}

	md.Active = active
	err = kv.writeGob(ctx, databasesMID, key, &md)
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

	return kv.updateDatabase(context.Background(), dbname, true)
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

	return kv.updateDatabase(context.Background(), dbname, false)
}

func makeSchemaKey(sn sql.SchemaName) []byte {
	return MakeSQLKey(
		[]sql.Value{
			sql.StringValue(sn.Database.String()),
			sql.StringValue(sn.Schema.String()),
		},
		schemasPrimary)
}

func (kv *KVRows) lookupSchema(tx *transaction, sn sql.SchemaName) error {
	if sn.Schema == sql.PUBLIC {
		return nil
	}
	return notImplemented
}

func (kv *KVRows) CreateSchema(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) error {

	if sn.Schema == sql.PUBLIC {
		return fmt.Errorf("kvrows: schema %s already exists", sn)
	}

	sqlKey := makeSchemaKey(sn)
	tx, err := kv.forWrite(ctx, etx, schemasMID, sqlKey)
	if err != nil {
		return nil
	}
	_ = tx

	return notImplemented
}

func (kv *KVRows) DropSchema(ctx context.Context, etx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	if sn.Schema == sql.PUBLIC {
		return fmt.Errorf("kvrows: schema %s may not be dropped", sn)
	}

	return notImplemented
}

func makeTableKey(tn sql.TableName) []byte {
	return MakeSQLKey(
		[]sql.Value{
			sql.StringValue(tn.Database.String()),
			sql.StringValue(tn.Schema.String()),
			sql.StringValue(tn.Table.String()),
		},
		tablesPrimary)
}

func (kv *KVRows) LookupTable(ctx context.Context, etx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	tx, err := kv.forRead(etx)
	if err != nil {
		return nil, err
	}
	err = kv.lookupSchema(tx, tn.SchemaName())
	if err != nil {
		return nil, err
	}

	/*
		keys, vals, err := kv.st.ReadRows(tx.key, tx.sid, tablesMID, makeTableKey(tn), nil,
			MaximumVersion)
		if err != nil {
			return nil, err
		}
		_ = keys
		_ = vals
	*/
	return nil, notImplemented
}

func (kv *KVRows) CreateTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	sqlKey := makeTableKey(tn)
	tx, err := kv.forWrite(ctx, etx, tablesMID, sqlKey)
	if err != nil {
		return err
	}
	err = kv.lookupSchema(tx, tn.SchemaName())
	if err != nil {
		return err
	}

	// XXX: WriteRows

	return notImplemented
}

func (kv *KVRows) DropTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	sqlKey := makeTableKey(tn)
	tx, err := kv.forWrite(ctx, etx, tablesMID, sqlKey)
	if err != nil {
		return err
	}
	err = kv.lookupSchema(tx, tn.SchemaName())
	if err != nil {
		return err
	}

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
	tid := atomic.AddUint64(&kv.lastTID, 1)

	return &transaction{
		kv: kv,
		key: TransactionKey{
			TID:   tid,
			Epoch: kv.epoch,
		},
		sid:   1,
		sesid: sesid,
		state: ActiveState,
	}
}

func (kv *KVRows) forRead(etx engine.Transaction) (*transaction, error) {
	tx := etx.(*transaction)
	if tx.state == CommittedState {
		return nil, errTransactionCommitted
	} else if tx.state == AbortedState {
		return nil, errTransactionAborted
	}
	return tx, nil
}

func (kv *KVRows) forWrite(ctx context.Context, etx engine.Transaction, mid uint64,
	sqlKey []byte) (*transaction, error) {

	tx := etx.(*transaction)
	if tx.state == CommittedState {
		return nil, errTransactionCommitted
	} else if tx.state == AbortedState {
		return nil, errTransactionAborted
	}
	if tx.key.MID > 0 && tx.key.Key != nil {
		return tx, nil
	}

	tx.key.MID = mid
	tx.key.Key = sqlKey
	md := transactionMetadata{
		State: ActiveState,
		Epoch: tx.key.Epoch,
	}
	err := kv.writeGob(ctx, mid, tx.key.EncodeKey(), &md)
	if err != nil {
		tx.state = AbortedState
		return nil, err
	}
	return tx, nil
}

func (kv *KVRows) finalizeTransaction(ctx context.Context, tx *transaction,
	ts TransactionState) error {

	if tx.state == CommittedState {
		return errTransactionCommitted
	} else if tx.state == AbortedState {
		return errTransactionAborted
	}
	if tx.key.MID == 0 || tx.key.Key == nil {
		tx.state = ts
		return nil
	}

	var md transactionMetadata
	key, err := kv.readGob(ctx, tx.key.MID, tx.key.EncodeKey(), &md)
	if err != nil {
		return err
	}

	if md.State == AbortedState {
		tx.state = AbortedState
		if ts == AbortedState {
			// Someone else already aborted the transaction for us; since we are rolling back
			// anyway, there is no error.
			return nil
		}
		return errTransactionAborted
	} else if md.State == CommittedState {
		// This should never happen: someone else committed the transaction for us.
		return fmt.Errorf("kvrows: internal error: transaction already committed: %v", tx)
	}

	md.State = ts
	err = kv.writeGob(ctx, tx.key.MID, key, &md)
	if err != nil {
		tx.state = AbortedState
		return err
	}

	tx.state = ts
	return nil
}

func (kv *KVRows) ListDatabases(ctx context.Context, tx engine.Transaction) ([]sql.Identifier,
	error) {

	return nil, notImplemented
}

func (kv *KVRows) ListSchemas(ctx context.Context, etx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	tx, err := kv.forRead(etx)
	if err != nil {
		return nil, err
	}
	_ = tx

	/*
		keys, vals, err := kv.st.ReadRows(tx.key, tx.sid, schemasMID, makeDatabaseKey(dbname), nil,
			MaximumVersion)
		if err != nil {
			return nil, err
		}
		_ = keys
		_ = vals
		// XXX
	*/

	scnames := []sql.Identifier{sql.PUBLIC}
	return scnames, nil
}

func (kv *KVRows) ListTables(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	return nil, notImplemented
}

func (tx *transaction) Commit(ctx context.Context) error {
	return tx.kv.finalizeTransaction(ctx, tx, CommittedState)
}

func (tx *transaction) Rollback() error {
	return tx.kv.finalizeTransaction(context.Background(), tx, AbortedState)
}

func (tx *transaction) NextStmt() {
	tx.sid += 1
}
