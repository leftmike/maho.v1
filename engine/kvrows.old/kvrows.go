package kvrows

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

const (
	configMID       = 0
	databasesMID    = 1
	transactionsMID = 2

	metadataKey = "metadata"
)

var (
	notImplemented          = errors.New("kvrows: not implemented")
	ErrKeyNotFound          = errors.New("kvrows: key not found")
	ErrValueVersionMismatch = errors.New("kvrows: value version mismatch")
)

type storeMetadata struct {
	Epoch   uint64
	Version uint64
}

type databaseMetadata struct {
	Active bool
}

type TransactionState byte

const (
	ActiveTransaction    TransactionState = 0
	CommittedTransaction TransactionState = 1
	AbortedTransaction   TransactionState = 2
)

type transactionMetadata struct {
	State   TransactionState
	Epoch   uint64
	Version uint64
}

type KVRows struct {
	mutex        sync.RWMutex
	st           Store
	epoch        uint64
	version      uint64
	lastTID      uint32
	databases    map[sql.Identifier]databaseMetadata
	transactions map[uint32]transactionMetadata
}

type transaction struct {
	kv      *KVRows
	tid     uint32
	sid     uint32
	sesid   uint64
	version uint64
	state   TransactionState
	keys    [][]byte
}

func getGob(m Mapper, key string, value interface{}) error {
	return m.Get([]byte(key),
		func(val []byte) error {
			dec := gob.NewDecoder(bytes.NewBuffer(val))
			return dec.Decode(value)
		})
}

func setGob(m Mapper, key string, value interface{}) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	if err != nil {
		return err
	}
	return m.Set([]byte(key), buf.Bytes())
}

func (kv *KVRows) loadMetadata() error {
	tx, err := kv.st.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(configMID)
	if err != nil {
		return err
	}

	var md storeMetadata
	err = getGob(m, metadataKey, &md)
	if err != nil {
		md.Epoch = 0
	}

	md.Epoch += 1
	kv.epoch = md.Epoch
	kv.version = md.Version
	err = setGob(m, metadataKey, &md)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (kv *KVRows) loadDatabases() error {
	vtbl := MakeVersionedTable(kv.st, databasesMID)
	return vtbl.List(
		func(key sql.Value, ver uint64, val []byte) error {
			s, ok := key.(sql.StringValue)
			if !ok {
				return fmt.Errorf("kvrows: databases: expected string key: %s", key)
			}

			var dm databaseMetadata
			if !ParseGobValue(val, &dm) {
				return fmt.Errorf("kvrows: databases: %s: unable to parse value: %v", s, val)
			}
			kv.databases[sql.ID(string(s))] = dm
			return nil
		})
}

func (kv *KVRows) loadTransactions() error {
	var abandoned []uint32
	kv.lastTID = 0

	vtbl := MakeVersionedTable(kv.st, transactionsMID)
	err := vtbl.List(
		func(key sql.Value, ver uint64, val []byte) error {
			i64, ok := key.(sql.Int64Value)
			if !ok {
				return fmt.Errorf("kvrows: transactions: expected int64 key: %s", key)
			}
			if i64 < 1 || i64 > math.MaxUint32 {
				return fmt.Errorf("kvrows: transactions: key out of range: %d", i64)
			}
			tid := uint32(i64)
			if tid > kv.lastTID {
				kv.lastTID = tid
			}

			var tm transactionMetadata
			if !ParseGobValue(val, &tm) {
				return fmt.Errorf("kvrows: transactions: %d: unable to parse value: %v", tid, val)
			}
			kv.transactions[tid] = tm
			if tm.State == ActiveTransaction {
				// We don't need to check the epoch, because this is happening at the beginning of
				// a new epoch, so all active transactions are from an older epoch and effectively
				// abandoned.
				abandoned = append(abandoned, tid)
			}
			return nil
		})
	if err != nil {
		return err
	}

	for _, tid := range abandoned {
		tm := kv.transactions[tid]
		tm.State = AbortedTransaction
		val, err := MakeGobValue(&tm)
		if err != nil {
			return err
		}
		err = vtbl.Set(sql.Int64Value(tid), 1, val)
		if err != nil {
			return err
		}
		kv.transactions[tid] = tm
	}

	if len(abandoned) > 0 {
		// XXX: start the cleaner
	}
	return nil
}

func (kv *KVRows) Startup(st Store) error {
	kv.st = st
	kv.databases = map[sql.Identifier]databaseMetadata{}
	kv.transactions = map[uint32]transactionMetadata{}

	err := kv.loadMetadata()
	if err != nil {
		return err
	}
	err = kv.loadDatabases()
	if err != nil {
		return err
	}
	err = kv.loadTransactions()
	if err != nil {
		return err
	}
	return nil
}

func (kv *KVRows) updateDatabase(dbname sql.Identifier, dm *databaseMetadata) error {
	vtbl := MakeVersionedTable(kv.st, databasesMID)
	key := sql.StringValue(dbname.String())
	ver, err := vtbl.Get(key,
		func(val []byte) error {
			return nil
		})
	if err != nil {
		ver = 0
	}
	val, err := MakeGobValue(dm)
	if err != nil {
		return err
	}
	return vtbl.Set(key, ver, val)
}

func (kv *KVRows) CreateDatabase(dbname sql.Identifier, options engine.Options) error {
	if len(options) != 0 {
		return fmt.Errorf("kvrows: unexpected option to create database: %s", dbname)
	}

	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	if ds, ok := kv.databases[dbname]; ok && ds.Active {
		return fmt.Errorf("kvrows: database %s already exists", dbname)
	}

	dm := databaseMetadata{
		Active: true,
	}
	err := kv.updateDatabase(dbname, &dm)
	if err != nil {
		return err
	}

	kv.databases[dbname] = dm
	return nil
}

func (kv *KVRows) DropDatabase(dbname sql.Identifier, ifExists bool, options engine.Options) error {
	if len(options) != 0 {
		return fmt.Errorf("kvrows: unexpected option to drop database: %s", dbname)
	}

	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	if ds, ok := kv.databases[dbname]; !ok || !ds.Active {
		if ifExists {
			return nil
		}
		return fmt.Errorf("kvrows: database %s does not exist", dbname)
	}

	dm := databaseMetadata{
		Active: false,
	}
	err := kv.updateDatabase(dbname, &dm)
	if err != nil {
		return err
	}

	kv.databases[dbname] = dm
	return nil
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

func (kv *KVRows) setTransactionState(tid uint32, state TransactionState) error {
	tm := transactionMetadata{
		State:   state,
		Epoch:   kv.epoch,
		Version: 0,
	}

	vtbl := MakeVersionedTable(kv.st, transactionsMID)
	ver := uint64(0)
	if state != ActiveTransaction {
		ver = 1
	}
	val, err := MakeGobValue(&tm)
	if err != nil {
		return err
	}
	err = vtbl.Set(sql.Int64Value(tid), ver, val)
	if err != nil {
		return err
	}

	kv.mutex.Lock()
	kv.transactions[tid] = tm
	kv.mutex.Unlock()

	return nil
}

func (kv *KVRows) saveTransaction(tid uint32, valVer uint64, tm transactionMetadata) error {
	vtbl := MakeVersionedTable(kv.st, transactionsMID)
	val, err := MakeGobValue(&tm)
	if err != nil {
		return err
	}
	err = vtbl.Set(sql.Int64Value(tid), valVer, val)
	if err != nil {
		return err
	}
	return nil
}

func (kv *KVRows) Begin(sesid uint64) engine.Transaction {
	tid := atomic.AddUint32(&kv.lastTID, 1)

	tm := transactionMetadata{
		State:   ActiveTransaction,
		Epoch:   kv.epoch,
		Version: 0,
	}
	err := kv.saveTransaction(tid, 0, tm)
	if err != nil {
		return nil // XXX: Begin should return an error
	}

	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	kv.transactions[tid] = tm

	return &transaction{
		kv:      kv,
		tid:     tid,
		sid:     1,
		sesid:   sesid,
		version: kv.version,
		state:   ActiveTransaction,
	}
}

func (kv *KVRows) commit(tx *transaction) error {
	kv.mutex.Lock()
	kv.version += 1
	ver := kv.version
	// XXX: need to update storeMetadata.version, though might be able to glean version from
	// committed transactions at startup and lazily write storeMetadata.version
	kv.mutex.Unlock()

	tm := transactionMetadata{
		State:   CommittedTransaction,
		Epoch:   kv.epoch,
		Version: ver,
	}
	err := kv.saveTransaction(tx.tid, 1, tm)
	if err != nil {
		return err
	}

	// At this point, the transaction is committed and durable.

	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	kv.transactions[tx.tid] = tm

	tx.state = CommittedTransaction
	return nil
}

func (kv *KVRows) rollback(tx *transaction) error {
	tm := transactionMetadata{
		State:   AbortedTransaction,
		Epoch:   kv.epoch,
		Version: 0,
	}
	err := kv.saveTransaction(tx.tid, 1, tm)
	if err != nil {
		return err
	}

	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	kv.transactions[tx.tid] = tm

	tx.state = AbortedTransaction
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

func (tx *transaction) Commit(ctx context.Context) error {
	return tx.kv.commit(tx)
}

func (tx *transaction) Rollback() error {
	return tx.kv.rollback(tx)
}

func (tx *transaction) NextStmt() {
	tx.sid += 1
}
