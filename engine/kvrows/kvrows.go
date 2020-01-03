package kvrows

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/typedtbl"
	"github.com/leftmike/maho/sql"
)

const (
	configMID       = 0
	databasesMID    = 1
	transactionsMID = 2

	schemasMID = 2048
	tablesMID  = 2049
)

var (
	notImplemented          = errors.New("kvrows: not implemented")
	errTransactionCommitted = errors.New("kvrows: transaction committed")
	errTransactionAborted   = errors.New("kvrows: transaction aborted")
	ErrKeyNotFound          = errors.New("kvrows: key not found")
	ErrValueVersionMismatch = errors.New("kvrows: value version mismatch")

	configKey = MakeMetadataKey([]sql.Value{sql.StringValue("metadata")})

	schemasTableName = sql.TableName{sql.ID("system"), sql.ID("private"), sql.ID("schemas")}
	schemasPrimary   = []engine.ColumnKey{
		engine.MakeColumnKey(0, false),
		engine.MakeColumnKey(1, false),
	}

	tablesTableName = sql.TableName{sql.ID("system"), sql.ID("private"), sql.ID("tables")}
	tablesPrimary   = []engine.ColumnKey{
		engine.MakeColumnKey(0, false),
		engine.MakeColumnKey(1, false),
		engine.MakeColumnKey(2, false),
	}
)

type storeMetadata struct {
	Node          uint32
	Epoch         uint32
	CommitVersion uint64
	LastMID       uint64
}

type databaseMetadata struct {
	Active bool
}

type KVRows struct {
	mutex         sync.RWMutex
	st            Store
	node          uint32
	epoch         uint32
	commitVersion uint64
	lastMID       uint64
	lastLocalID   uint64
	databases     map[sql.Identifier]databaseMetadata
	transactions  map[TransactionID]*transaction
}

type TransactionState byte

const (
	ActiveState    TransactionState = 0
	CommittedState TransactionState = 1
	AbortedState   TransactionState = 2
	UnknownState   TransactionState = 3
)

type transactionMetadata struct {
	State         TransactionState
	TID           TransactionID
	CommitVersion uint64
}

type transaction struct {
	kv         *KVRows
	tid        TransactionID
	sid        uint64
	sesid      uint64
	hasWritten bool

	mutex         sync.RWMutex
	done          chan struct{}
	state         TransactionState
	commitVersion uint64
}

func (kv *KVRows) readGob(ctx context.Context, mid uint64, key []byte, value interface{}) (uint64,
	error) {

	ver, val, err := kv.st.ReadValue(ctx, mid, key)
	if err != nil {
		return 0, err
	}

	dec := gob.NewDecoder(bytes.NewBuffer(val))
	return ver, dec.Decode(value)
}

func (kv *KVRows) writeGob(ctx context.Context, mid uint64, key []byte, ver uint64,
	value interface{}) error {

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	if err != nil {
		return err
	}
	return kv.st.WriteValue(ctx, mid, key, ver, buf.Bytes())
}

func (kv *KVRows) loadConfig(ctx context.Context) error {
	var md storeMetadata
	ver, err := kv.readGob(ctx, configMID, configKey, &md)
	if err == ErrKeyNotFound {
		md = storeMetadata{
			Node:          1,
			Epoch:         0,
			CommitVersion: 0,
			LastMID:       4095, // The first non-system table will be 4096.
		}
	} else if err != nil {
		return err
	}

	kv.node = md.Node
	md.Epoch += 1
	kv.epoch = md.Epoch
	kv.commitVersion = md.CommitVersion
	kv.lastMID = md.LastMID
	return kv.writeGob(ctx, configMID, configKey, ver, &md)
}

func (kv *KVRows) saveConfig(ctx context.Context) error {
	var md storeMetadata
	ver, err := kv.readGob(ctx, configMID, configKey, &md)
	if err != nil {
		return err
	}

	md.CommitVersion = kv.commitVersion
	md.LastMID = kv.lastMID
	return kv.writeGob(ctx, configMID, configKey, ver, &md)
}

func (kv *KVRows) loadDatabases(ctx context.Context) error {
	return kv.st.ListValues(ctx, databasesMID,
		func(key []byte, ver uint64, val []byte) (bool, error) {
			mdKey := []sql.Value{nil}
			ok := ParseMetadataKey(key, mdKey)
			if !ok {
				return false, fmt.Errorf("kvrows: databases: corrupt primary key: %v", key)
			}
			s, ok := mdKey[0].(sql.StringValue)
			if !ok {
				return false, fmt.Errorf("kvrows: databases: expected string key: %s", mdKey[0])
			}

			var md databaseMetadata
			dec := gob.NewDecoder(bytes.NewBuffer(val))
			err := dec.Decode(&md)
			if err != nil {
				return false, err
			}

			kv.databases[sql.ID(string(s))] = md
			return false, nil
		})
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
	err := kv.loadConfig(ctx)
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

func makeDatabaseKey(dbname sql.Identifier) []byte {
	return MakeMetadataKey([]sql.Value{sql.StringValue(dbname.String())})
}

func (kv *KVRows) updateDatabase(ctx context.Context, dbname sql.Identifier, active bool) error {
	key := makeDatabaseKey(dbname)
	var md databaseMetadata
	ver, err := kv.readGob(ctx, databasesMID, key, &md)
	if err != nil && err != ErrKeyNotFound {
		return err
	}

	md.Active = active
	err = kv.writeGob(ctx, databasesMID, key, ver, &md)
	if err != nil {
		return err
	}
	kv.databases[dbname] = md
	return nil
}

func (kv *KVRows) createDatabase(ctx context.Context, dbname sql.Identifier) error {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	md, ok := kv.databases[dbname]
	if ok && md.Active {
		return fmt.Errorf("kvrows: database %s already exists", dbname)
	}

	return kv.updateDatabase(ctx, dbname, true)
}

func (kv *KVRows) setupDatabase(ctx context.Context, dbname sql.Identifier) error {
	etx := kv.Begin(0)
	err := kv.CreateSchema(ctx, etx, sql.SchemaName{dbname, sql.PUBLIC})
	if err != nil {
		etx.Rollback()
		return err
	}
	return etx.Commit(ctx)
}

func (kv *KVRows) CreateDatabase(dbname sql.Identifier, options engine.Options) error {
	if len(options) != 0 {
		return fmt.Errorf("kvrows: unexpected option to create database: %s", dbname)
	}

	ctx := context.Background()
	err := kv.createDatabase(ctx, dbname)
	if err != nil {
		return err
	}
	err = kv.setupDatabase(ctx, dbname)
	if err != nil {
		kv.updateDatabase(ctx, dbname, false)
		return err
	}
	return nil
}

func (kv *KVRows) cleanupDatabase(ctx context.Context, dbname sql.Identifier) error {
	etx := kv.Begin(0)
	scnames, err := kv.ListSchemas(ctx, etx, dbname)
	if err != nil {
		etx.Rollback()
		return err
	}
	for _, scname := range scnames {
		err = kv.DropSchema(ctx, etx, sql.SchemaName{dbname, scname}, true)
		if err != nil {
			etx.Rollback()
			return err
		}
	}
	return etx.Commit(ctx)
}

func (kv *KVRows) DropDatabase(dbname sql.Identifier, ifExists bool, options engine.Options) error {
	if len(options) != 0 {
		return fmt.Errorf("kvrows: unexpected option to drop database: %s", dbname)
	}

	ctx := context.Background()
	err := kv.cleanupDatabase(ctx, dbname)
	if err != nil {
		return err
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

	return kv.updateDatabase(ctx, dbname, false)
}

type schemaRow struct {
	Database string
	Schema   string
	Tables   int64
}

func (kv *KVRows) makeSchemasTable(tx *transaction) *typedtbl.Table {
	return typedtbl.MakeTable(schemasTableName,
		&table{
			kv:       kv,
			tx:       tx,
			mid:      schemasMID,
			cols:     []sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("tables")},
			colTypes: []sql.ColumnType{sql.IdColType, sql.IdColType, sql.Int64ColType},
			primary:  schemasPrimary,
		})
}

func (kv *KVRows) CreateSchema(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) error {

	tx, err := kv.forWrite(ctx, etx)
	if err != nil {
		return nil
	}

	kv.mutex.Lock()
	md, ok := kv.databases[sn.Database]
	kv.mutex.Unlock()

	if !ok || !md.Active {
		return fmt.Errorf("kvrows: database %s not found", sn.Database)
	}

	ttbl := kv.makeSchemasTable(tx)
	return ttbl.Insert(ctx,
		schemaRow{
			Database: sn.Database.String(),
			Schema:   sn.Schema.String(),
			Tables:   0,
		})
}

func (kv *KVRows) DropSchema(ctx context.Context, etx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	tx, err := kv.forWrite(ctx, etx)
	if err != nil {
		return nil
	}

	ttbl := kv.makeSchemasTable(tx)
	rows, err := ttbl.Seek(ctx,
		[]sql.Value{sql.StringValue(sn.Database.String()), sql.StringValue(sn.Schema.String())})
	if err != nil {
		return err
	}
	defer rows.Close()

	var sr schemaRow
	err = rows.Next(ctx, &sr)
	if err == io.EOF {
		if ifExists {
			return nil
		}
		return fmt.Errorf("kvrows: schema %s not found", sn)
	} else if err != nil {
		return err
	}

	if sr.Database != sn.Database.String() || sr.Schema != sn.Schema.String() {
		if ifExists {
			return nil
		}
		return fmt.Errorf("kvrows: schema %s not found", sn)
	}
	if sr.Tables > 0 {
		return fmt.Errorf("kvrows: schema %s is not empty", sn)
	}
	return rows.Delete(ctx)
}

func (kv *KVRows) updateSchema(ctx context.Context, tx *transaction, sn sql.SchemaName,
	delta int64) error {

	ttbl := kv.makeSchemasTable(tx)
	rows, err := ttbl.Seek(ctx,
		[]sql.Value{sql.StringValue(sn.Database.String()), sql.StringValue(sn.Schema.String())})
	if err != nil {
		return err
	}
	defer rows.Close()

	var sr schemaRow
	err = rows.Next(ctx, &sr)
	if err == io.EOF {
		return fmt.Errorf("kvrows: schema %s not found", sn)
	} else if err != nil {
		return err
	}

	if sr.Database != sn.Database.String() || sr.Schema != sn.Schema.String() {
		return fmt.Errorf("kvrows: schema %s not found", sn)
	}
	return rows.Update(ctx,
		struct {
			Tables int64
		}{sr.Tables + delta})
}

type tableRow struct {
	Database string
	Schema   string
	Table    string
	MID      int64
}

func (kv *KVRows) makeTablesTable(tx *transaction) *typedtbl.Table {
	return typedtbl.MakeTable(tablesTableName,
		&table{
			kv:  kv,
			tx:  tx,
			mid: tablesMID,
			cols: []sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("table"),
				sql.ID("mid")},
			colTypes: []sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType,
				sql.Int64ColType},
			primary: tablesPrimary,
		})
}

func (kv *KVRows) lookupTable(ctx context.Context, tx *transaction, tn sql.TableName) (uint64,
	error) {

	ttbl := kv.makeTablesTable(tx)
	rows, err := ttbl.Seek(ctx,
		[]sql.Value{
			sql.StringValue(tn.Database.String()),
			sql.StringValue(tn.Schema.String()),
			sql.StringValue(tn.Table.String()),
		})
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var tr tableRow
	err = rows.Next(ctx, &tr)
	if err == io.EOF {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	if tr.Database != tn.Database.String() || tr.Schema != tn.Schema.String() ||
		tr.Table != tn.Table.String() {

		return 0, nil
	}

	return uint64(tr.MID), nil
}

func (kv *KVRows) LookupTable(ctx context.Context, etx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	tx, err := kv.forRead(etx)
	if err != nil {
		return nil, err
	}

	mid, err := kv.lookupTable(ctx, tx, tn)
	if err != nil {
		return nil, err
	} else if mid == 0 {
		return nil, fmt.Errorf("kvrows: table %s not found", tn)
	}

	var cols []sql.Identifier
	var colTypes []sql.ColumnType
	var primary []engine.ColumnKey
	_, err = kv.readGob(ctx, mid, MakeMetadataKey([]sql.Value{sql.StringValue("columns")}), &cols)
	if err != nil {
		return nil, err
	}
	_, err = kv.readGob(ctx, mid, MakeMetadataKey([]sql.Value{sql.StringValue("column-types")}),
		&colTypes)
	if err != nil {
		return nil, err
	}
	_, err = kv.readGob(ctx, mid, MakeMetadataKey([]sql.Value{sql.StringValue("primary")}),
		&primary)
	if err != nil {
		return nil, err
	}

	return &table{
		kv:       kv,
		tx:       tx,
		mid:      mid,
		cols:     cols,
		colTypes: colTypes,
		primary:  primary,
	}, nil
}

func (kv *KVRows) allocateMID(ctx context.Context) (uint64, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	kv.lastMID += 1
	return kv.lastMID, kv.saveConfig(ctx)
}

func (kv *KVRows) CreateTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	tx, err := kv.forWrite(ctx, etx)
	if err != nil {
		return err
	}

	mid, err := kv.lookupTable(ctx, tx, tn)
	if err != nil {
		return err
	}
	if mid > 0 {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("kvrows: table %s already exists", tn)
	}

	err = kv.updateSchema(ctx, tx, tn.SchemaName(), 1)
	if err != nil {
		return err
	}

	mid, err = kv.allocateMID(ctx)
	if err != nil {
		return err
	}

	ttbl := kv.makeTablesTable(tx)
	err = ttbl.Insert(ctx,
		tableRow{
			Database: tn.Database.String(),
			Schema:   tn.Schema.String(),
			Table:    tn.Table.String(),
			MID:      int64(mid),
		})
	if err != nil {
		return err
	}

	err = kv.writeGob(ctx, mid, MakeMetadataKey([]sql.Value{sql.StringValue("columns")}), 0, &cols)
	if err != nil {
		return err
	}
	err = kv.writeGob(ctx, mid, MakeMetadataKey([]sql.Value{sql.StringValue("column-types")}), 0,
		&colTypes)
	if err != nil {
		return err
	}
	return kv.writeGob(ctx, mid, MakeMetadataKey([]sql.Value{sql.StringValue("primary")}), 0,
		&primary)
}

func (kv *KVRows) DropTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	tx, err := kv.forWrite(ctx, etx)
	if err != nil {
		return err
	}

	err = kv.updateSchema(ctx, tx, tn.SchemaName(), -1)
	if err != nil {
		return err
	}

	ttbl := kv.makeTablesTable(tx)
	rows, err := ttbl.Seek(ctx,
		[]sql.Value{
			sql.StringValue(tn.Database.String()),
			sql.StringValue(tn.Schema.String()),
			sql.StringValue(tn.Table.String()),
		})
	if err != nil {
		return err
	}
	defer rows.Close()

	var tr tableRow
	err = rows.Next(ctx, &tr)
	if err == io.EOF {
		if ifExists {
			return nil
		}
		return fmt.Errorf("kvrows: table %s not found", tn)
	} else if err != nil {
		return err
	}

	if tr.Database != tn.Database.String() || tr.Schema != tn.Schema.String() ||
		tr.Table != tn.Table.String() {

		if ifExists {
			return nil
		}
		return fmt.Errorf("kvrows: table %s not found", tn)
	}
	return rows.Delete(ctx)
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
		hasWritten: false, // Assume read-only until proven otherwise.

		done:  make(chan struct{}),
		state: ActiveState,
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

func (tx *transaction) makeKey() []byte {
	return MakeMetadataKey([]sql.Value{sql.Int64Value(tx.tid.Node), sql.Int64Value(tx.tid.Epoch),
		sql.Int64Value(tx.tid.LocalID)})
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
	err := kv.writeGob(ctx, transactionsMID, tx.makeKey(), 0, &md)
	if err != nil {
		tx.setState(AbortedState, 0)
		return nil, err
	}

	tx.hasWritten = true
	return tx, nil
}

func (kv *KVRows) allocateCommitVersion(ctx context.Context) (uint64, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	kv.commitVersion += 1
	return kv.commitVersion, kv.saveConfig(ctx)
}

func (kv *KVRows) finalizeTransaction(ctx context.Context, tx *transaction,
	ts TransactionState) error {

	if tx.state == CommittedState {
		return errTransactionCommitted
	} else if tx.state == AbortedState {
		return errTransactionAborted
	}
	if !tx.hasWritten {
		tx.setState(ts, 0)
		return nil
	}

	var md transactionMetadata
	key := tx.makeKey()
	ver, err := kv.readGob(ctx, transactionsMID, key, &md)
	if err != nil {
		tx.setState(AbortedState, 0)
		return err
	}

	if md.State == AbortedState {
		tx.setState(AbortedState, 0)
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

	var commitVersion uint64
	if ts == CommittedState {
		commitVersion, err = kv.allocateCommitVersion(ctx)
		if err != nil {
			tx.setState(AbortedState, 0)
			return err
		}
	}

	md.State = ts
	md.CommitVersion = commitVersion
	err = kv.writeGob(ctx, transactionsMID, key, ver, &md)
	if err != nil {
		tx.setState(AbortedState, 0)
		return err
	}

	tx.setState(ts, commitVersion)
	return nil
}

func (kv *KVRows) getState(tid TransactionID) (TransactionState, uint64) {
	kv.mutex.RLock()
	tx, ok := kv.transactions[tid]
	kv.mutex.RUnlock()
	if !ok {
		return UnknownState, 0
	}

	tx.mutex.RLock()
	defer tx.mutex.RUnlock()
	return tx.state, tx.commitVersion
}

func (tx *transaction) setState(st TransactionState, ver uint64) {
	if tx.state == ActiveState {
		tx.mutex.Lock()
		defer tx.mutex.Unlock()

		tx.state = st
		tx.commitVersion = ver
		close(tx.done)
	}
}

func (kv *KVRows) waitOnTID(ctx context.Context, tid TransactionID) error {
	kv.mutex.RLock()
	tx, ok := kv.transactions[tid]
	kv.mutex.RUnlock()
	if !ok {
		// XXX: unknown transactions are assumed aborted?
		return fmt.Errorf("kvrows: unknown transaction: %v", tid)
	}

	<-tx.done
	return nil
}

func (kv *KVRows) ListDatabases(ctx context.Context, tx engine.Transaction) ([]sql.Identifier,
	error) {

	kv.mutex.RLock()
	defer kv.mutex.RUnlock()

	var dbnames []sql.Identifier
	for db, md := range kv.databases {
		if md.Active {
			dbnames = append(dbnames, db)
		}
	}
	return dbnames, nil
}

func (kv *KVRows) ListSchemas(ctx context.Context, etx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	tx, err := kv.forRead(etx)
	if err != nil {
		return nil, err
	}

	ttbl := kv.makeSchemasTable(tx)
	rows, err := ttbl.Seek(ctx, []sql.Value{sql.StringValue(dbname.String()), sql.StringValue("")})
	if err != nil {
		return nil, err
	}

	var scnames []sql.Identifier
	for {
		var sr schemaRow
		err = rows.Next(ctx, &sr)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if sr.Database != dbname.String() {
			break
		}
		scnames = append(scnames, sql.ID(sr.Schema))
	}
	return scnames, nil
}

func (kv *KVRows) ListTables(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	tx, err := kv.forRead(etx)
	if err != nil {
		return nil, err
	}

	ttbl := kv.makeTablesTable(tx)
	rows, err := ttbl.Seek(ctx,
		[]sql.Value{
			sql.StringValue(sn.Database.String()),
			sql.StringValue(sn.Schema.String()),
			sql.StringValue(""),
		})
	if err != nil {
		return nil, err
	}

	var tblnames []sql.Identifier
	for {
		var tr tableRow
		err = rows.Next(ctx, &tr)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if tr.Database != sn.Database.String() || tr.Schema != sn.Schema.String() {
			break
		}
		tblnames = append(tblnames, sql.ID(tr.Table))
	}
	return tblnames, nil
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
