package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/leftmike/maho/flags"
	"github.com/leftmike/maho/sql"
)

type Engine struct {
	mutex            sync.RWMutex
	st               store
	flgs             flags.Flags
	systemInfoTables map[sql.Identifier]sql.MakeVirtual
	metadataTables   map[sql.Identifier]sql.MakeVirtual
}

func NewEngine(st store, flgs flags.Flags) sql.Engine {
	e := &Engine{
		st:               st,
		flgs:             flgs,
		systemInfoTables: map[sql.Identifier]sql.MakeVirtual{},
		metadataTables:   map[sql.Identifier]sql.MakeVirtual{},
	}

	e.CreateSystemInfoTable(sql.DATABASES, e.makeDatabasesTable)
	e.CreateSystemInfoTable(sql.ID("identifiers"), makeIdentifiersTable)

	e.CreateMetadataTable(sql.COLUMNS, e.makeColumnsTable)
	e.CreateMetadataTable(sql.CONSTRAINTS, e.makeConstraintsTable)
	e.CreateMetadataTable(sql.SCHEMAS, e.makeSchemasTable)
	e.CreateMetadataTable(sql.TABLES, e.makeTablesTable)

	return e
}

func (e *Engine) GetFlag(f flags.Flag) bool {
	return e.flgs.GetFlag(f)
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

func (e *Engine) ValidDatabase(dbname sql.Identifier) (bool, error) {
	if dbname == sql.SYSTEM {
		return true, nil
	}
	return e.st.ValidDatabase(dbname)
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

func (e *Engine) lookupVirtualTable(ctx context.Context, tx sql.Transaction,
	tn sql.TableName) (sql.Table, sql.TableType, error, bool) {

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if tn.Schema == sql.METADATA {
		if maker, ok := e.metadataTables[tn.Table]; ok {
			tbl, tt, err := maker(ctx, tx, tn)
			return tbl, tt, err, true
		}
		return nil, nil, fmt.Errorf("engine: table %s not found", tn), true
	} else if tn.Database == sql.SYSTEM && tn.Schema == sql.INFO {
		if maker, ok := e.systemInfoTables[tn.Table]; ok {
			tbl, tt, err := maker(ctx, tx, tn)
			return tbl, tt, err, true
		}
		return nil, nil, fmt.Errorf("engine: table %s not found", tn), true
	}

	return nil, nil, nil, false
}
