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

type metadata struct {
	Epoch uint64
}

type databaseState struct {
	Active bool
}

const (
	activeTransaction    = 0
	committedTransaction = 1
	abortedTransaction   = 2
)

type transactionState struct {
	State byte
	Epoch uint64
}

type KVRows struct {
	mutex        sync.RWMutex
	st           Store
	epoch        uint64
	lastTID      uint32
	databases    map[sql.Identifier]databaseState
	transactions map[uint32]transactionState
}

type transaction struct {
	kv    *KVRows
	tid   uint32
	sid   uint32
	sesid uint64
	state byte
	keys  [][]byte
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

	var md metadata
	err = getGob(m, metadataKey, &md)
	if err != nil {
		md.Epoch = 0
	}

	md.Epoch += 1
	kv.epoch = md.Epoch
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

			var ds databaseState
			if !ParseGobValue(val, &ds) {
				return fmt.Errorf("kvrows: databases: %s: unable to parse value: %v", s, val)
			}
			kv.databases[sql.ID(string(s))] = ds
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

			var ts transactionState
			if !ParseGobValue(val, &ts) {
				return fmt.Errorf("kvrows: transactions: %d: unable to parse value: %v", tid, val)
			}
			kv.transactions[tid] = ts
			if ts.State == activeTransaction {
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
		ts := kv.transactions[tid]
		ts.State = abortedTransaction
		val, err := MakeGobValue(&ts)
		if err != nil {
			return err
		}
		err = vtbl.Set(sql.Int64Value(tid), 1, val)
		if err != nil {
			return err
		}
		kv.transactions[tid] = ts
	}

	if len(abandoned) > 0 {
		// XXX: start the cleaner
	}
	return nil
}

func (kv *KVRows) Startup(st Store) error {
	kv.st = st
	kv.databases = map[sql.Identifier]databaseState{}
	kv.transactions = map[uint32]transactionState{}

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

func (kv *KVRows) updateDatabase(dbname sql.Identifier, ds *databaseState) error {
	vtbl := MakeVersionedTable(kv.st, databasesMID)
	key := sql.StringValue(dbname.String())
	ver, err := vtbl.Get(key,
		func(val []byte) error {
			return nil
		})
	if err != nil {
		ver = 0
	}
	val, err := MakeGobValue(ds)
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

	ds := databaseState{
		Active: true,
	}
	err := kv.updateDatabase(dbname, &ds)
	if err != nil {
		return err
	}

	kv.databases[dbname] = ds
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

	ds := databaseState{
		Active: false,
	}
	err := kv.updateDatabase(dbname, &ds)
	if err != nil {
		return err
	}

	kv.databases[dbname] = ds
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

func (kv *KVRows) setTransactionState(tid uint32, state byte) error {
	ts := transactionState{
		State: state,
		Epoch: kv.epoch,
	}

	vtbl := MakeVersionedTable(kv.st, transactionsMID)
	ver := uint64(0)
	if state != activeTransaction {
		ver = 1
	}
	val, err := MakeGobValue(&ts)
	if err != nil {
		return err
	}
	err = vtbl.Set(sql.Int64Value(tid), ver, val)
	if err != nil {
		return err
	}

	kv.mutex.Lock()
	kv.transactions[tid] = ts
	kv.mutex.Unlock()

	return nil
}

func (kv *KVRows) Begin(sesid uint64) engine.Transaction {
	tx := &transaction{
		kv:    kv,
		tid:   atomic.AddUint32(&kv.lastTID, 1),
		sid:   1,
		sesid: sesid,
		state: activeTransaction,
	}
	err := kv.setTransactionState(tx.tid, activeTransaction)
	if err != nil {
		return nil // XXX: Begin should return an error
	}
	return tx
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
	err := tx.kv.setTransactionState(tx.tid, committedTransaction)
	if err != nil {
		return err
	}
	tx.state = committedTransaction
	return nil
}

func (tx *transaction) Rollback() error {
	err := tx.kv.setTransactionState(tx.tid, abortedTransaction)
	if err != nil {
		return err
	}
	tx.state = abortedTransaction
	return nil
}

func (tx *transaction) NextStmt() {
	tx.sid += 1
}
