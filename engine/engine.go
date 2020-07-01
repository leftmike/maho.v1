package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

type Engine struct {
	mutex            sync.RWMutex
	st               *storage.Store
	systemInfoTables map[sql.Identifier]sql.MakeVirtual
	metadataTables   map[sql.Identifier]sql.MakeVirtual
}

func NewEngine(st *storage.Store) (*Engine, error) {
	e := &Engine{
		st:               st,
		systemInfoTables: map[sql.Identifier]sql.MakeVirtual{},
		metadataTables:   map[sql.Identifier]sql.MakeVirtual{},
	}

	e.CreateSystemInfoTable(sql.ID("config"), makeConfigTable)
	e.CreateSystemInfoTable(sql.DATABASES, e.makeDatabasesTable)
	e.CreateSystemInfoTable(sql.ID("identifiers"), makeIdentifiersTable)

	e.CreateMetadataTable(sql.COLUMNS, e.makeColumnsTable)
	e.CreateMetadataTable(sql.SCHEMAS, e.makeSchemasTable)
	e.CreateMetadataTable(sql.TABLES, e.makeTablesTable)

	return e, nil
}

func (e *Engine) CreateSystemInfoTable(tblname sql.Identifier, maker sql.MakeVirtual) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if _, ok := e.systemInfoTables[tblname]; ok {
		panic(fmt.Sprintf("system info table already created: %s", tblname))
	}
	e.systemInfoTables[tblname] = maker
}

func (e *Engine) CreateMetadataTable(tblname sql.Identifier, maker sql.MakeVirtual) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if _, ok := e.metadataTables[tblname]; ok {
		panic(fmt.Sprintf("metadata table already created: %s", tblname))
	}
	e.metadataTables[tblname] = maker
}

func (e *Engine) CreateDatabase(dbname sql.Identifier, options map[sql.Identifier]string) error {
	if dbname == sql.SYSTEM {
		return fmt.Errorf("engine: database %s already exists", dbname)
	}
	return e.st.CreateDatabase(dbname, options)
}

func (e *Engine) DropDatabase(dbname sql.Identifier, ifExists bool,
	options map[sql.Identifier]string) error {

	if dbname == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be dropped", dbname)
	}
	return e.st.DropDatabase(dbname, ifExists, options)
}

func (e *Engine) CreateSchema(ctx context.Context, tx sql.Transaction, sn sql.SchemaName) error {
	if sn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", sn.Database)
	}
	if sn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s already exists", sn)
	}
	return e.st.CreateSchema(ctx, tx, sn)
}

func (e *Engine) DropSchema(ctx context.Context, tx sql.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	if sn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", sn.Database)
	}
	if sn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be dropped", sn)
	}
	return e.st.DropSchema(ctx, tx, sn, ifExists)
}

func (e *Engine) LookupTable(ctx context.Context, tx sql.Transaction, tn sql.TableName) (sql.Table,
	sql.TableType, error) {

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if tn.Schema == sql.METADATA {
		if maker, ok := e.metadataTables[tn.Table]; ok {
			return maker(ctx, tx, tn)
		}
		return nil, nil, fmt.Errorf("engine: table %s not found", tn)
	} else if tn.Database == sql.SYSTEM && tn.Schema == sql.INFO {
		if maker, ok := e.systemInfoTables[tn.Table]; ok {
			return maker(ctx, tx, tn)
		}
		return nil, nil, fmt.Errorf("engine: table %s not found", tn)
	}

	st, err := e.st.LookupTable(ctx, tx, tn)
	if err != nil {
		return nil, nil, err
	}
	tt := MakeTableType(st.Columns(ctx), st.ColumnTypes(ctx), st.PrimaryKey(ctx))
	return makeTable(tn, st, tt)
}

func (e *Engine) CreateTable(ctx context.Context, tx sql.Transaction, tn sql.TableName,
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

func (e *Engine) DropTable(ctx context.Context, tx sql.Transaction, tn sql.TableName,
	ifExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}
	return e.st.DropTable(ctx, tx, tn, ifExists)
}

func (e *Engine) CreateIndex(ctx context.Context, tx sql.Transaction, idxname sql.Identifier,
	tn sql.TableName, unique bool, keys []sql.ColumnKey, ifNotExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}
	return e.st.CreateIndex(ctx, tx, idxname, tn, unique, keys, ifNotExists)
}

func (e *Engine) DropIndex(ctx context.Context, tx sql.Transaction, idxname sql.Identifier,
	tn sql.TableName, ifExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}
	return e.st.DropIndex(ctx, tx, idxname, tn, ifExists)
}

func (e *Engine) Begin(sesid uint64) sql.Transaction {
	return e.st.Begin(sesid)
}

func (e *Engine) ListDatabases(ctx context.Context, tx sql.Transaction) ([]sql.Identifier, error) {
	return e.st.ListDatabases(ctx, tx)
}
