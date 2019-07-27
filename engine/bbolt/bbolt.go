package bbolt

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/service"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"

	"go.etcd.io/bbolt"
)

var (
	errNotEmpty = errors.New("not empty")
)

type bboltEngine struct {
	lockService service.LockService
	mutex       sync.RWMutex
	databases   map[sql.Identifier]*database
	dataDir     string
	lastTID     uint64
}

type database struct {
	db   *bbolt.DB
	path string
	name sql.Identifier
}

type transaction struct {
	e           *bboltEngine
	db          *database
	tx          *bbolt.Tx
	tid         uint64
	sid         uint64
	lockerState service.LockerState
}

type table struct {
	b           *bbolt.Bucket
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
}

func NewEngine(dataDir string) engine.Engine {
	be := &bboltEngine{
		databases: map[sql.Identifier]*database{},
		dataDir:   dataDir,
	}
	ve := virtual.NewEngine(be)
	be.lockService.Init(ve)
	return ve
}

func (_ *bboltEngine) CreateSystemTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("bbolt: use virtual engine with bbolt engine")
}

func (_ *bboltEngine) CreateInfoTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("bbolt: use virtual engine with bbolt engine")
}

func databasePath(dbname sql.Identifier, dataDir, ext string, options engine.Options) string {
	var path string
	if optionPath, ok := options[sql.PATH]; ok {
		path = optionPath
	} else {
		path = filepath.Join(dataDir, dbname.String())
	}

	if filepath.Ext(path) == "" {
		path += ext
	}
	return path
}

func (be *bboltEngine) CreateDatabase(dbname sql.Identifier, options engine.Options) error {
	be.mutex.Lock()
	defer be.mutex.Unlock()

	if _, ok := be.databases[dbname]; ok {
		return fmt.Errorf("bbolt: database %s already exists", dbname)
	}
	path := databasePath(dbname, be.dataDir, ".mahobbolt", options)
	db, err := bbolt.Open(path, 0644, nil)
	if err != nil {
		return fmt.Errorf("bbolt: create database %s failed: %s", dbname, err)
	}

	err = db.Update(
		func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucket([]byte(sql.PUBLIC.String()))
			return err
		})
	if err != nil {
		return fmt.Errorf("bbolt: create database %s failed: %s", dbname, err)
	}

	be.databases[dbname] = &database{
		db:   db,
		path: path,
		name: dbname,
	}
	return nil
}

func (be *bboltEngine) DropDatabase(dbname sql.Identifier, ifExists bool,
	options engine.Options) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	_, ok := be.databases[dbname]
	if !ok {
		if ifExists {
			return nil
		}
		return fmt.Errorf("bbolt: database %s does not exist", dbname)
	}
	delete(be.databases, dbname)
	return nil
}

func (be *bboltEngine) CreateSchema(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[sn.Database]
	if !ok {
		return fmt.Errorf("bbolt: database %s not found", sn.Database)
	}
	return bdb.createSchema(ctx, tx, sn)
}

func (be *bboltEngine) DropSchema(ctx context.Context, tx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[sn.Database]
	if !ok {
		return fmt.Errorf("bbolt: database %s not found", sn.Database)
	}
	return bdb.dropSchema(ctx, tx, sn, ifExists)
}

func (be *bboltEngine) LookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return nil, fmt.Errorf("bbolt: database %s not found", tn.Database)
	}
	return bdb.lookupTable(ctx, tx, tn)
}

func (be *bboltEngine) CreateTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return fmt.Errorf("bbolt: database %s not found", tn.Database)
	}
	return bdb.createTable(ctx, tx, tn, cols, colTypes, primary, ifNotExists)
}

func (be *bboltEngine) DropTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return fmt.Errorf("bbolt: database %s not found", tn.Database)
	}
	return bdb.dropTable(ctx, tx, tn, ifExists)
}

func (_ *bboltEngine) CreateIndex(ctx context.Context, tx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, unique bool, keys []engine.ColumnKey,
	ifNotExists bool) error {

	return errors.New("bbolt: create index not implemented") // XXX
}

func (_ *bboltEngine) DropIndex(ctx context.Context, tx engine.Transaction, idxname sql.Identifier,
	tn sql.TableName, ifExists bool) error {

	return errors.New("bbolt: drop index not implemented") // XXX
}

func (be *bboltEngine) Begin(sid uint64) engine.Transaction {
	return &transaction{
		e:   be,
		tid: atomic.AddUint64(&be.lastTID, 1),
		sid: sid,
	}
}

func (_ *bboltEngine) IsTransactional() bool {
	return true
}

func (be *bboltEngine) ListDatabases(ctx context.Context, tx engine.Transaction) ([]sql.Identifier,
	error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	var dbnames []sql.Identifier
	for dbname := range be.databases {
		dbnames = append(dbnames, dbname)
	}
	return dbnames, nil
}

func (be *bboltEngine) ListSchemas(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[dbname]
	if !ok {
		return nil, fmt.Errorf("bbolt: database %s not found", dbname)
	}

	return bdb.listSchemas(ctx, tx)
}

func (be *bboltEngine) ListTables(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[sn.Database]
	if !ok {
		return nil, fmt.Errorf("bbolt: database %s not found", sn.Database)
	}

	return bdb.listTables(ctx, tx, sn)
}

func (tx *transaction) Commit(ctx context.Context) error {
	if tx.tx != nil {
		tx.e.lockService.ReleaseLocks(tx)
		err := tx.tx.Commit()
		if err != nil {
			return fmt.Errorf("bbolt: unable to commit transaction: %s", err)
		}
		tx.tx = nil
	}
	return nil
}

func (tx *transaction) Rollback() error {
	if tx.tx != nil {
		tx.e.lockService.ReleaseLocks(tx)
		err := tx.tx.Rollback()
		if err != nil {
			return fmt.Errorf("bbolt: unable to rollback transaction: %s", err)
		}
		tx.tx = nil
	}
	return nil
}

func (_ *transaction) NextStmt() {}

func (tx *transaction) LockerState() *service.LockerState {
	return &tx.lockerState
}

func (tx *transaction) String() string {
	return fmt.Sprintf("transaction-%d", tx.tid)
}

func (tx *transaction) lockSchema(ctx context.Context, sn sql.SchemaName,
	ll service.LockLevel) error {

	return tx.e.lockService.LockSchema(ctx, tx, sn, ll)
}

func (tx *transaction) lockTable(ctx context.Context, tn sql.TableName,
	ll service.LockLevel) error {

	return tx.e.lockService.LockTable(ctx, tx, tn, ll)
}

func (bdb *database) transaction(etx engine.Transaction) (*transaction, error) {
	tx := etx.(*transaction)
	if tx.db == nil {
		var err error
		tx.tx, err = bdb.db.Begin(true)
		if err != nil {
			return nil, fmt.Errorf("bbolt: unable to begin transaction: %s", err)
		}
		tx.db = bdb
	} else if tx.db != bdb {
		return nil, fmt.Errorf("bbolt: multiple database transactions not allowed: %s and %s",
			tx.db.name, bdb.name)
	}
	return tx, nil
}

func (bdb *database) createSchema(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) error {

	tx, err := bdb.transaction(etx)
	if err != nil {
		return err
	}

	err = tx.lockSchema(ctx, sn, service.EXCLUSIVE)
	if err != nil {
		return err
	}

	sb := tx.tx.Bucket([]byte(sn.Schema.String()))
	if sb != nil {
		return fmt.Errorf("bbolt: schema %s already exists", sn)
	}

	_, err = tx.tx.CreateBucket([]byte(sn.Schema.String()))
	if err != nil {
		return fmt.Errorf("bbolt: unable to create schema %s: %s", sn, err)
	}
	return nil
}

func (bdb *database) dropSchema(ctx context.Context, etx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	tx, err := bdb.transaction(etx)
	if err != nil {
		return err
	}

	err = tx.lockSchema(ctx, sn, service.EXCLUSIVE)
	if err != nil {
		return err
	}

	sb := tx.tx.Bucket([]byte(sn.Schema.String()))
	if sb == nil {
		if ifExists {
			return nil
		}
		return fmt.Errorf("bbolt: schema %s does not exist", sn)
	}

	err = sb.ForEach(
		func(key, val []byte) error {
			if val == nil && !hidden(key) {
				return errNotEmpty
			}
			return nil
		})
	if err != nil {
		return fmt.Errorf("bbolt: schema %s is not empty", sn)
	}

	err = tx.tx.DeleteBucket([]byte(sn.Schema.String()))
	if err != nil {
		return fmt.Errorf("bbolt: unable to drop schema %s: %s", sn, err)
	}
	return nil
}

func hidden(key []byte) bool {
	return len(key) > 0 && key[0] == 0
}

func hiddenKey(key string) []byte {
	return append([]byte{0}, []byte(key)...)
}

func putGob(b *bbolt.Bucket, key string, val interface{}) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	if err != nil {
		return err
	}
	return b.Put(hiddenKey(key), buf.Bytes())
}

func getGob(b *bbolt.Bucket, key string, val interface{}) error {
	bval := b.Get(hiddenKey(key))
	if bval == nil {
		return fmt.Errorf("bbolt: key %s not found", key)
	}
	dec := gob.NewDecoder(bytes.NewBuffer(bval))
	return dec.Decode(val)
}

func (bdb *database) lookupTable(ctx context.Context, etx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	tx, err := bdb.transaction(etx)
	if err != nil {
		return nil, err
	}

	err = tx.lockTable(ctx, tn, service.ACCESS)
	if err != nil {
		return nil, err
	}

	sb := tx.tx.Bucket([]byte(tn.Schema.String()))
	if sb == nil {
		return nil, fmt.Errorf("bbolt: schema %s not found", tn.SchemaName())
	}

	tb := sb.Bucket([]byte(tn.Table.String()))
	if tb == nil {
		return nil, fmt.Errorf("bbolt: table %s not found", tn)
	}

	var cols []sql.Identifier
	var colTypes []sql.ColumnType
	err = getGob(tb, "columns", &cols)
	if err != nil {
		return nil, fmt.Errorf("bbolt: unable to lookup table %s: %s", tn, err)
	}
	err = getGob(tb, "types", &colTypes)
	if err != nil {
		return nil, fmt.Errorf("bbolt: unable to lookup table %s: %s", tn, err)
	}

	return &table{
		b:           tb,
		columns:     cols,
		columnTypes: colTypes,
	}, nil
}

func (bdb *database) createTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	tx, err := bdb.transaction(etx)
	if err != nil {
		return err
	}

	err = tx.lockSchema(ctx, tn.SchemaName(), service.ACCESS)
	if err != nil {
		return err
	}
	err = tx.lockTable(ctx, tn, service.EXCLUSIVE)
	if err != nil {
		return err
	}

	sb := tx.tx.Bucket([]byte(tn.Schema.String()))
	if sb == nil {
		return fmt.Errorf("bbolt: schema %s not found", tn.SchemaName())
	}

	tb := sb.Bucket([]byte(tn.Table.String()))
	if tb != nil {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("bbolt: table %s already exists", tn)
	}

	tb, err = sb.CreateBucket([]byte(tn.Table.String()))
	if err != nil {
		return fmt.Errorf("bbolt: unable to create table %s: %s", tn, err)
	}
	err = putGob(tb, "columns", &cols)
	if err != nil {
		return fmt.Errorf("bbolt: unable to create table %s: %s", tn, err)
	}
	err = putGob(tb, "types", &colTypes)
	if err != nil {
		return fmt.Errorf("bbolt: unable to create table %s: %s", tn, err)
	}

	return nil
}

func (bdb *database) dropTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	tx, err := bdb.transaction(etx)
	if err != nil {
		return err
	}

	err = tx.lockSchema(ctx, tn.SchemaName(), service.ACCESS)
	if err != nil {
		return err
	}
	err = tx.lockTable(ctx, tn, service.EXCLUSIVE)
	if err != nil {
		return err
	}

	sb := tx.tx.Bucket([]byte(tn.Schema.String()))
	if sb == nil {
		if ifExists {
			return nil
		}
		return fmt.Errorf("bbolt: schema %s not found", tn.SchemaName())
	}

	err = sb.DeleteBucket([]byte(tn.Table.String()))
	if err != nil {
		if ifExists {
			return nil
		}
		return fmt.Errorf("bbolt: table %s not found", tn)
	}
	return nil
}

func (bdb *database) listSchemas(ctx context.Context, etx engine.Transaction) ([]sql.Identifier,
	error) {

	tx, err := bdb.transaction(etx)
	if err != nil {
		return nil, err
	}

	var schemas []sql.Identifier
	err = tx.tx.ForEach(
		func(name []byte, bkt *bbolt.Bucket) error {
			if !hidden(name) {
				schemas = append(schemas, sql.ID(string(name)))
			}
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("bbolt: unable to list schemas: %s", err)
	}
	return schemas, nil
}

func (bdb *database) listTables(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	tx, err := bdb.transaction(etx)
	if err != nil {
		return nil, err
	}

	sb := tx.tx.Bucket([]byte(sn.Schema.String()))
	if sb == nil {
		return nil, fmt.Errorf("bbolt: schema %s not found", sn)
	}

	var tables []sql.Identifier
	err = sb.ForEach(
		func(key, val []byte) error {
			if val == nil && !hidden(key) {
				tables = append(tables, sql.ID(string(key)))
			}
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("bbolt: unable to list tables: %s", err)
	}
	return tables, nil
}

func (bt *table) Columns(ctx context.Context) []sql.Identifier {
	return bt.columns
}

func (bt *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return bt.columnTypes
}

func (bt *table) Rows(ctx context.Context) (engine.Rows, error) {
	return nil, errors.New("not implemented") // XXX
	/*
		bt.be.mutex.RLock()
		defer bt.be.mutex.RUnlock()

		return &rows{be: bt.be, tn: bt.tn, columns: bt.columns, rows: bt.rows}, nil
	*/
}

func (bt *table) Insert(ctx context.Context, row []sql.Value) error {
	return errors.New("not implemented") // XXX
	/*
		bt.be.mutex.Lock()
		defer bt.be.mutex.Unlock()

		bt.rows = append(bt.rows, row)
		return nil
	*/
}

/*
func (br *rows) Columns() []sql.Identifier {
	return br.columns
}

func (br *rows) Close() error {
	br.index = len(br.rows)
	br.haveRow = false
	return nil
}

func (br *rows) Next(ctx context.Context, dest []sql.Value) error {
	br.be.mutex.RLock()
	defer br.be.mutex.RUnlock()

	for br.index < len(br.rows) {
		if br.rows[br.index] != nil {
			copy(dest, br.rows[br.index])
			br.index += 1
			br.haveRow = true
			return nil
		}
		br.index += 1
	}

	br.haveRow = false
	return io.EOF
}

func (br *rows) Delete(ctx context.Context) error {
	br.be.mutex.Lock()
	defer br.be.mutex.Unlock()

	if !br.haveRow {
		return fmt.Errorf("basic: table %s no row to delete", br.tn)
	}
	br.haveRow = false
	br.rows[br.index-1] = nil
	return nil
}

func (br *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	br.be.mutex.Lock()
	defer br.be.mutex.Unlock()

	if !br.haveRow {
		return fmt.Errorf("basic: table %s no row to update", br.tn)
	}
	row := br.rows[br.index-1]
	for _, up := range updates {
		row[up.Index] = up.Value
	}
	return nil
}
*/
/*
type readTx struct {
	tx   *bbolt.Tx
	done bool
}

type writeTx struct {
	readTx
}

type iterator struct {
	cursor *bbolt.Cursor
	prefix []byte
	key    []byte
	val    []byte
}

func (Engine) Open(path string) (kv.DB, error) {
	path, err := kv.FixPath(path, ".mahobbolt", "bbolt")
	if err != nil {
		return nil, err
	}
	db, err := bbolt.Open(path, 0644, nil)
	if err != nil {
		return nil, err
	}
	return database{db}, nil
}

func (db database) ReadTx() (kv.ReadTx, error) {
	tx, err := db.db.Begin(false)
	if err != nil {
		return nil, err
	}
	return &readTx{tx: tx}, nil
}

func (db database) WriteTx() (kv.WriteTx, error) {
	tx, err := db.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &writeTx{readTx{tx: tx}}, nil
}

func (db database) Close() error {
	return db.db.Close()
}

func (rtx *readTx) Discard() {
	if rtx.done {
		return
	}
	rtx.done = true
	err := rtx.tx.Rollback()
	if err != nil {
		panic(fmt.Sprintf("bbolt.Rollback() failed"))
	}
}

func (rtx *readTx) Get(key []byte, vf func(val []byte) error) error {
	val, err := rtx.GetValue(key)
	if err != nil {
		return err
	}
	return vf(val)
}

func getBucket(tx *bbolt.Tx) *bbolt.Bucket {
	return tx.Bucket(bucketName)
}

func (rtx *readTx) GetValue(key []byte) ([]byte, error) {
	bkt := getBucket(rtx.tx)
	if bkt == nil {
		return nil, kv.ErrKeyNotFound
	}
	val := bkt.Get(key)
	if val == nil {
		return nil, kv.ErrKeyNotFound
	}
	return val, nil
}

func (rtx *readTx) Iterate(prefix []byte) kv.Iterator {
	var cursor *bbolt.Cursor
	bkt := getBucket(rtx.tx)
	if bkt == nil {
		cursor = rtx.tx.Cursor()
	} else {
		cursor = bkt.Cursor()
	}
	return &iterator{
		cursor: cursor,
		prefix: prefix,
	}
}

func (wtx *writeTx) Commit() error {
	if wtx.done {
		return nil
	}
	wtx.done = true
	return wtx.tx.Commit()
}

func (wtx *writeTx) Delete(key []byte) error {
	bkt := getBucket(wtx.tx)
	if bkt == nil {
		return kv.ErrKeyNotFound
	}
	return bkt.Delete(key)
}

func (wtx *writeTx) Set(key []byte, val []byte) error {
	bkt, err := wtx.tx.CreateBucketIfNotExists(bucketName)
	if err != nil {
		return err
	}
	return bkt.Put(key, val)
}

func (it *iterator) Close() {
	// Nothing.
}

func (it *iterator) Key() []byte {
	return it.key
}

func (it *iterator) KeyCopy() []byte {
	return it.key
}

func (it *iterator) setKeyVal(key, val []byte) {
	if key != nil && bytes.HasPrefix(key, it.prefix) {
		it.key = key
		it.val = val
	} else {
		it.key = nil
	}
}

func (it *iterator) Next() {
	it.setKeyVal(it.cursor.Next())
}

func (it *iterator) Rewind() {
	it.setKeyVal(it.cursor.Seek(it.prefix))
}

func (it *iterator) Seek(key []byte) {
	it.setKeyVal(it.cursor.Seek(key))
}

func (it *iterator) Valid() bool {
	return it.key != nil
}

func (it *iterator) Value(vf func(val []byte) error) error {
	return vf(it.val)
}
*/
