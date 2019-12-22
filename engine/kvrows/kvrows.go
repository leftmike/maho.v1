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
	configMID       = 0
	databasesMID    = 1
	transactionsMID = 2

	schemasMID = 2048
	tablesMID  = 2049

	firstUserMID = 4096
)

var (
	notImplemented          = errors.New("kvrows: not implemented")
	errTransactionCommitted = errors.New("kvrows: transaction committed")
	errTransactionAborted   = errors.New("kvrows: transaction aborted")
	ErrKeyNotFound          = errors.New("kvrows: key not found")
	ErrValueVersionMismatch = errors.New("kvrows: value version mismatch")

	metadataPrimary = []engine.ColumnKey{engine.MakeColumnKey(0, false)}
	metadataKey     = Key{
		SQLKey: MakeSQLKey([]sql.Value{sql.StringValue("metadata")}, metadataPrimary),
	}

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
	transactionsPrimary = []engine.ColumnKey{
		engine.MakeColumnKey(0, false),
		engine.MakeColumnKey(1, false),
		engine.MakeColumnKey(2, false),
	}
)

type storeMetadata struct {
	Node    uint32
	Epoch   uint32
	Version uint64
}

type databaseMetadata struct {
	Active bool
}

type KVRows struct {
	mutex        sync.RWMutex
	st           Store
	node         uint32
	epoch        uint32
	version      uint64
	lastLocalID  uint64
	databases    map[sql.Identifier]databaseMetadata
	transactions map[TransactionID]*transaction
}

type TransactionState byte

const (
	ActiveState    TransactionState = 0
	CommittedState TransactionState = 1
	AbortedState   TransactionState = 2
	UnknownState   TransactionState = 3
)

type transactionMetadata struct {
	State            TransactionState
	TID              TransactionID
	CommittedVersion uint64
}

type transaction struct {
	kv         *KVRows
	tid        TransactionID
	sid        uint64
	sesid      uint64
	state      TransactionState
	hasWritten bool
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

	kv.node = md.Node
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
		ok := ParseSQLKey(keys[idx].SQLKey, databasesPrimary, sqlKey)
		if !ok {
			return fmt.Errorf("kvrows: databases: corrupt primary key: %v", keys[idx].SQLKey)
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

func (kv *KVRows) loadTransactions(ctx context.Context) error {
	// XXX
	return nil
}

func (kv *KVRows) Startup(st Store) error {
	kv.st = st
	kv.databases = map[sql.Identifier]databaseMetadata{}
	kv.transactions = map[TransactionID]*transaction{}

	ctx := context.Background()
	err := kv.loadMetadata(ctx)
	if err != nil {
		return err
	}
	err = kv.loadDatabases(ctx)
	if err != nil {
		return err
	}
	err = kv.loadTransactions(ctx)
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
		SQLKey: makeDatabaseKey(dbname),
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

func (kv *KVRows) CreateSchema(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) error {

	if sn.Schema == sql.PUBLIC {
		return fmt.Errorf("kvrows: schema %s already exists", sn)
	}

	tx, err := kv.forWrite(ctx, etx)
	if err != nil {
		return nil
	}

	row := []sql.Value{
		sql.StringValue(sn.Database.String()),
		sql.StringValue(sn.Schema.String()),
		sql.Int64Value(0),
	}
	return kv.st.InsertMap(ctx, kv.getState, tx.tid, tx.sid, schemasMID, makeSchemaKey(sn),
		MakeRowValue(row))
}

func (kv *KVRows) lookupSchemaKey(ctx context.Context, tx *transaction, sn sql.SchemaName) (Key,
	int64, error) {

	/*
		sqlKey := makeSchemaKey(sn)
		keys, vals, _, err := kv.st.ScanRelation(ctx, kv.getState, tx.tid, tx.sid, schemasMID,
			MaximumVersion, 1, sqlKey)
		if err != nil {
			return Key{}, 0, err
		} else if len(keys) == 0 || !bytes.Equal(sqlKey, keys[0].SQLKey) {
			return Key{}, 0, io.EOF
		}
		row := []sql.Value{nil, nil, nil}
		if !ParseRowValue(vals[0], row) {
			return Key{}, 0, fmt.Errorf("kvrows: at key %v unable to parse row: %v", keys[0], vals[0])
		}
		i64, ok := row[2].(sql.Int64Value)
		if !ok {
			return Key{}, 0, fmt.Errorf("kvrows: schemas table: expected an int, got %s", row[2])
		}
		return keys[0], int64(i64), nil
	*/
	return Key{}, 0, notImplemented
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
	_ = tx
	/*
		keys, vals, err := kv.st.ReadRows(tx.tid, tx.sid, tablesMID, makeTableKey(tn), nil,
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

	tx, err := kv.forWrite(ctx, etx)
	if err != nil {
		return err
	}
	_ = tx
	//sqlKey := makeTableKey(tn)
	// XXX: WriteRows

	return notImplemented
}

func (kv *KVRows) DropTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	tx, err := kv.forWrite(ctx, etx)
	if err != nil {
		return err
	}
	_ = tx
	//sqlKey := makeTableKey(tn)

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
	lid := atomic.AddUint64(&kv.lastLocalID, 1)

	tx := &transaction{
		kv: kv,
		tid: TransactionID{
			Node:    kv.node,
			Epoch:   kv.epoch,
			LocalID: lid,
		},
		sid:        1,
		sesid:      sesid,
		state:      ActiveState,
		hasWritten: false, // Assume read-only until proven otherwise.
	}

	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	kv.transactions[tx.tid] = tx
	return tx
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

func (tx *transaction) makeKey() Key {
	return Key{
		SQLKey: MakeSQLKey([]sql.Value{sql.Int64Value(tx.tid.Node), sql.Int64Value(tx.tid.Epoch),
			sql.Int64Value(tx.tid.LocalID)}, transactionsPrimary),
	}
}

func (kv *KVRows) forWrite(ctx context.Context, etx engine.Transaction) (*transaction, error) {
	tx := etx.(*transaction)
	if tx.state == CommittedState {
		return nil, errTransactionCommitted
	} else if tx.state == AbortedState {
		return nil, errTransactionAborted
	}
	if tx.hasWritten {
		return tx, nil
	}

	md := transactionMetadata{
		State: ActiveState,
		TID:   tx.tid,
	}
	err := kv.writeGob(ctx, transactionsMID, tx.makeKey(), &md)
	if err != nil {
		tx.state = AbortedState
		return nil, err
	}

	tx.hasWritten = true
	return tx, nil
}

func (kv *KVRows) finalizeTransaction(ctx context.Context, tx *transaction,
	ts TransactionState) error {

	if tx.state == CommittedState {
		return errTransactionCommitted
	} else if tx.state == AbortedState {
		return errTransactionAborted
	}
	if !tx.hasWritten {
		tx.state = ts
		return nil
	}

	var md transactionMetadata
	key, err := kv.readGob(ctx, transactionsMID, tx.makeKey(), &md)
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
	err = kv.writeGob(ctx, transactionsMID, key, &md)
	if err != nil {
		tx.state = AbortedState
		return err
	}

	tx.state = ts
	return nil
}

func (kv *KVRows) getState(tid TransactionID) (TransactionState, uint64) {
	kv.mutex.RLock()
	defer kv.mutex.RUnlock()

	tx, ok := kv.transactions[tid]
	if !ok {
		return UnknownState, 0
	}
	return tx.state, 0 // XXX: need the commit version in the tx
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

	/*
		keys, vals, _, err := kv.st.ScanRelation(ctx, kv.getState, tx.tid, tx.sid, schemasMID,
			MaximumVersion, math.MaxInt32, nil)
		if err != io.EOF && err != nil {
			return nil, err
		}
	*/
	_ = tx
	scnames := []sql.Identifier{sql.PUBLIC} // XXX: sql.PUBLIC
	/*
		for idx, val := range vals {
			row := []sql.Value{nil, nil, nil}
			if !ParseRowValue(val, row) {
				return nil, fmt.Errorf("kvrows: at key %v unable to parse row: %v", keys[idx], val)
			}
			s, ok := row[0].(sql.StringValue)
			if !ok {
				return nil, fmt.Errorf("kvrows: schemas table: expected a string, got %s", row[0])
			}
			if string(s) != dbname.String() {
				continue
			}
			s, ok = row[1].(sql.StringValue)
			if !ok {
				return nil, fmt.Errorf("kvrows: schemas table: expected a string, got %s", row[1])
			}
			scnames = append(scnames, sql.QuotedID(string(s)))
		}
	*/
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
