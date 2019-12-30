package bbolt

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/localkv"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
)

type bboltEngine struct {
	mutex     sync.RWMutex
	databases map[sql.Identifier]*kvrows.KVRows
	dataDir   string
}

type bboltTransaction struct {
	sesid uint64
	db    *kvrows.KVRows
	tx    engine.Transaction
}

func NewEngine(dataDir string) (engine.Engine, error) {
	be := &bboltEngine{
		databases: map[sql.Identifier]*kvrows.KVRows{},
		dataDir:   dataDir,
	}
	ve := virtual.NewEngine(be)
	return ve, nil
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
		delete(options, sql.PATH)
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
	st, err := OpenStore(path)
	if err != nil {
		return fmt.Errorf("bbolt: create database %s failed: %s", dbname, err)
	}

	var kv kvrows.KVRows
	err = kv.Startup(localkv.NewStore(st))
	if err != nil {
		return err
	}
	err = kv.CreateDatabase(dbname, options)
	if err != nil {
		return err
	}
	be.databases[dbname] = &kv
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
	delete(be.databases, dbname) // XXX need to close the store
	return nil
}

func (be *bboltEngine) CreateSchema(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) error {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[sn.Database]
	if !ok {
		return fmt.Errorf("bbolt: database %s not found", sn.Database)
	}
	tx, ok := toKVRowsTransaction(etx, bdb)
	if !ok {
		return fmt.Errorf("bbolt: multiple database transactions not allowed: %s", sn.Database)
	}
	return bdb.CreateSchema(ctx, tx, sn)
}

func (be *bboltEngine) DropSchema(ctx context.Context, etx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[sn.Database]
	if !ok {
		return fmt.Errorf("bbolt: database %s not found", sn.Database)
	}
	tx, ok := toKVRowsTransaction(etx, bdb)
	if !ok {
		return fmt.Errorf("bbolt: multiple database transactions not allowed: %s", sn.Database)
	}
	return bdb.DropSchema(ctx, tx, sn, ifExists)
}

func (be *bboltEngine) LookupTable(ctx context.Context, etx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return nil, fmt.Errorf("bbolt: database %s not found", tn.Database)
	}
	tx, ok := toKVRowsTransaction(etx, bdb)
	if !ok {
		return nil, fmt.Errorf("bbolt: multiple database transactions not allowed: %s", tn.Database)
	}
	return bdb.LookupTable(ctx, tx, tn)
}

func (be *bboltEngine) CreateTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return fmt.Errorf("bbolt: database %s not found", tn.Database)
	}
	tx, ok := toKVRowsTransaction(etx, bdb)
	if !ok {
		return fmt.Errorf("bbolt: multiple database transactions not allowed: %s", tn.Database)
	}
	return bdb.CreateTable(ctx, tx, tn, cols, colTypes, primary, ifNotExists)
}

func (be *bboltEngine) DropTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return fmt.Errorf("bbolt: database %s not found", tn.Database)
	}
	tx, ok := toKVRowsTransaction(etx, bdb)
	if !ok {
		return fmt.Errorf("bbolt: multiple database transactions not allowed: %s", tn.Database)
	}
	return bdb.DropTable(ctx, tx, tn, ifExists)
}

func (be *bboltEngine) CreateIndex(ctx context.Context, etx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, unique bool, keys []engine.ColumnKey,
	ifNotExists bool) error {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return fmt.Errorf("bbolt: database %s not found", tn.Database)
	}
	tx, ok := toKVRowsTransaction(etx, bdb)
	if !ok {
		return fmt.Errorf("bbolt: multiple database transactions not allowed: %s", tn.Database)
	}
	return bdb.CreateIndex(ctx, tx, idxname, tn, unique, keys, ifNotExists)
}

func (be *bboltEngine) DropIndex(ctx context.Context, etx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, ifExists bool) error {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return fmt.Errorf("bbolt: database %s not found", tn.Database)
	}
	tx, ok := toKVRowsTransaction(etx, bdb)
	if !ok {
		return fmt.Errorf("bbolt: multiple database transactions not allowed: %s", tn.Database)
	}
	return bdb.DropIndex(ctx, tx, idxname, tn, ifExists)
}

func (be *bboltEngine) Begin(sesid uint64) engine.Transaction {
	return &bboltTransaction{
		sesid: sesid,
	}
}

func (_ *bboltEngine) IsTransactional() bool {
	return true
}

func (be *bboltEngine) ListDatabases(ctx context.Context, etx engine.Transaction) ([]sql.Identifier,
	error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	var dbnames []sql.Identifier
	for dbname := range be.databases {
		dbnames = append(dbnames, dbname)
	}
	return dbnames, nil
}

func (be *bboltEngine) ListSchemas(ctx context.Context, etx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[dbname]
	if !ok {
		return nil, fmt.Errorf("bbolt: database %s not found", dbname)
	}
	tx, ok := toKVRowsTransaction(etx, bdb)
	if !ok {
		return nil, fmt.Errorf("bbolt: multiple database transactions not allowed: %s", dbname)
	}
	return bdb.ListSchemas(ctx, tx, dbname)
}

func (be *bboltEngine) ListTables(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[sn.Database]
	if !ok {
		return nil, fmt.Errorf("bbolt: database %s not found", sn.Database)
	}
	tx, ok := toKVRowsTransaction(etx, bdb)
	if !ok {
		return nil, fmt.Errorf("bbolt: multiple database transactions not allowed: %s", sn.Database)
	}
	return bdb.ListTables(ctx, tx, sn)
}

func toKVRowsTransaction(etx engine.Transaction, db *kvrows.KVRows) (engine.Transaction, bool) {
	tx := etx.(*bboltTransaction)
	if tx.db == nil {
		tx.tx = db.Begin(tx.sesid)
		tx.db = db
	} else if tx.db != db {
		return nil, false
	}
	return tx.tx, true
}

func (btx *bboltTransaction) Commit(ctx context.Context) error {
	if btx.tx == nil {
		return nil
	}
	return btx.tx.Commit(ctx)
}

func (btx *bboltTransaction) Rollback() error {
	if btx.tx == nil {
		return nil
	}
	return btx.tx.Rollback()
}

func (btx *bboltTransaction) NextStmt() {
	if btx.tx != nil {
		btx.tx.NextStmt()
	}
}
