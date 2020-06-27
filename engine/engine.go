package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

type MakeVirtual func(ctx context.Context, tx Transaction, tn sql.TableName) (Table, error)

type Transaction interface {
	storage.Transaction
}

type Rows interface {
	storage.Rows
}

type Table interface {
	storage.Table
}

type Engine interface {
	CreateSystemInfoTable(tblname sql.Identifier, maker MakeVirtual)
	CreateMetadataTable(tblname sql.Identifier, maker MakeVirtual)

	CreateDatabase(dbname sql.Identifier, options map[sql.Identifier]string) error
	DropDatabase(dbname sql.Identifier, ifExists bool, options map[sql.Identifier]string) error

	CreateSchema(ctx context.Context, tx Transaction, sn sql.SchemaName) error
	DropSchema(ctx context.Context, tx Transaction, sn sql.SchemaName, ifExists bool) error

	LookupTable(ctx context.Context, tx Transaction, tn sql.TableName) (Table, error)
	CreateTable(ctx context.Context, tx Transaction, tn sql.TableName, cols []sql.Identifier,
		colTypes []sql.ColumnType, primary []sql.ColumnKey, ifNotExists bool) error
	DropTable(ctx context.Context, tx Transaction, tn sql.TableName, ifExists bool) error

	CreateIndex(ctx context.Context, tx Transaction, idxname sql.Identifier, tn sql.TableName,
		unique bool, keys []sql.ColumnKey, ifNotExists bool) error
	DropIndex(ctx context.Context, tx Transaction, idxname sql.Identifier, tn sql.TableName,
		ifExists bool) error

	Begin(sesid uint64) Transaction

	ListDatabases(ctx context.Context, tx Transaction) ([]sql.Identifier, error)
	ListSchemas(ctx context.Context, tx Transaction, dbname sql.Identifier) ([]sql.Identifier, error)
	ListTables(ctx context.Context, tx Transaction, sn sql.SchemaName) ([]sql.Identifier, error)
}

type engine struct {
	mutex            sync.RWMutex
	st               storage.Store
	systemInfoTables map[sql.Identifier]MakeVirtual
	metadataTables   map[sql.Identifier]MakeVirtual
}

func NewEngine(st storage.Store) Engine {
	e := &engine{
		st:               st,
		systemInfoTables: map[sql.Identifier]MakeVirtual{},
		metadataTables:   map[sql.Identifier]MakeVirtual{},
	}

	e.CreateSystemInfoTable(sql.ID("config"), makeConfigTable)
	e.CreateSystemInfoTable(sql.DATABASES, e.makeDatabasesTable)
	e.CreateSystemInfoTable(sql.ID("identifiers"), makeIdentifiersTable)

	e.CreateMetadataTable(sql.COLUMNS, e.makeColumnsTable)
	e.CreateMetadataTable(sql.SCHEMAS, e.makeSchemasTable)
	e.CreateMetadataTable(sql.TABLES, e.makeTablesTable)

	return e
}

func (e *engine) CreateSystemInfoTable(tblname sql.Identifier, maker MakeVirtual) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if _, ok := e.systemInfoTables[tblname]; ok {
		panic(fmt.Sprintf("system info table already created: %s", tblname))
	}
	e.systemInfoTables[tblname] = maker
}

func (e *engine) CreateMetadataTable(tblname sql.Identifier, maker MakeVirtual) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if _, ok := e.metadataTables[tblname]; ok {
		panic(fmt.Sprintf("metadata table already created: %s", tblname))
	}
	e.metadataTables[tblname] = maker
}

func (e *engine) CreateDatabase(dbname sql.Identifier, options map[sql.Identifier]string) error {
	if dbname == sql.SYSTEM {
		return fmt.Errorf("engine: database %s already exists", dbname)
	}
	return e.st.CreateDatabase(dbname, options)
}

func (e *engine) DropDatabase(dbname sql.Identifier, ifExists bool,
	options map[sql.Identifier]string) error {

	if dbname == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be dropped", dbname)
	}
	return e.st.DropDatabase(dbname, ifExists, options)
}

func (e *engine) CreateSchema(ctx context.Context, tx Transaction, sn sql.SchemaName) error {
	if sn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", sn.Database)
	}
	if sn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s already exists", sn)
	}
	return e.st.CreateSchema(ctx, tx, sn)
}

func (e *engine) DropSchema(ctx context.Context, tx Transaction, sn sql.SchemaName,
	ifExists bool) error {

	if sn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", sn.Database)
	}
	if sn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be dropped", sn)
	}
	return e.st.DropSchema(ctx, tx, sn, ifExists)
}

func (e *engine) LookupTable(ctx context.Context, tx Transaction, tn sql.TableName) (Table, error) {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if tn.Schema == sql.METADATA {
		if maker, ok := e.metadataTables[tn.Table]; ok {
			return maker(ctx, tx, tn)
		}
		return nil, fmt.Errorf("engine: table %s not found", tn)
	} else if tn.Database == sql.SYSTEM && tn.Schema == sql.INFO {
		if maker, ok := e.systemInfoTables[tn.Table]; ok {
			return maker(ctx, tx, tn)
		}
		return nil, fmt.Errorf("engine: table %s not found", tn)
	}

	return e.st.LookupTable(ctx, tx, tn)
}

func (e *engine) CreateTable(ctx context.Context, tx Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []sql.ColumnKey,
	ifNotExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}
	return e.st.CreateTable(ctx, tx, tn, cols, colTypes, primary, ifNotExists)
}

func (e *engine) DropTable(ctx context.Context, tx Transaction, tn sql.TableName,
	ifExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}
	return e.st.DropTable(ctx, tx, tn, ifExists)
}

func (e *engine) CreateIndex(ctx context.Context, tx Transaction, idxname sql.Identifier,
	tn sql.TableName, unique bool, keys []sql.ColumnKey, ifNotExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}
	return e.st.CreateIndex(ctx, tx, idxname, tn, unique, keys, ifNotExists)
}

func (e *engine) DropIndex(ctx context.Context, tx Transaction, idxname sql.Identifier,
	tn sql.TableName, ifExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}
	return e.st.DropIndex(ctx, tx, idxname, tn, ifExists)
}

func (e *engine) Begin(sesid uint64) Transaction {
	return e.st.Begin(sesid)
}

func (e *engine) ListDatabases(ctx context.Context, tx Transaction) ([]sql.Identifier, error) {
	return e.st.ListDatabases(ctx, tx)
}

func (e *engine) ListSchemas(ctx context.Context, tx Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	return e.st.ListSchemas(ctx, tx, dbname)
}

func (e *engine) ListTables(ctx context.Context, tx Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	return e.st.ListTables(ctx, tx, sn)
}
