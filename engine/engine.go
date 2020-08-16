package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

type Table interface {
	Rows(ctx context.Context, minRow, maxRow []sql.Value) (Rows, error)
	Insert(ctx context.Context, row []sql.Value) error
}

type Rows interface {
	Columns() []sql.Identifier
	Close() error
	Next(ctx context.Context) ([]sql.Value, error)
	Delete(ctx context.Context) error
	Update(ctx context.Context, updatedCols []int, updateRow []sql.Value) error
}

type store interface {
	CreateDatabase(dbname sql.Identifier, options map[sql.Identifier]string) error
	DropDatabase(dbname sql.Identifier, ifExists bool, options map[sql.Identifier]string) error

	CreateSchema(ctx context.Context, tx sql.Transaction, sn sql.SchemaName) error
	DropSchema(ctx context.Context, tx sql.Transaction, sn sql.SchemaName, ifExists bool) error

	LookupTable(ctx context.Context, tx sql.Transaction, tn sql.TableName) (Table, *TableType,
		error)
	CreateTable(ctx context.Context, tx sql.Transaction, tn sql.TableName, tt *TableType,
		ifNotExists bool) error
	DropTable(ctx context.Context, tx sql.Transaction, tn sql.TableName, ifExists bool) error
	UpdateType(ctx context.Context, tx sql.Transaction, tn sql.TableName, tt *TableType) error

	MakeIndexType(tt *TableType, nam sql.Identifier, key []sql.ColumnKey,
		unique bool) sql.IndexType
	AddIndex(ctx context.Context, tx sql.Transaction, tn sql.TableName, tt *TableType,
		it sql.IndexType) error
	RemoveIndex(ctx context.Context, tx sql.Transaction, tn sql.TableName, tt *TableType,
		rdx int) error

	ListDatabases(ctx context.Context, tx sql.Transaction) ([]sql.Identifier, error)
	ListSchemas(ctx context.Context, tx sql.Transaction, dbname sql.Identifier) ([]sql.Identifier,
		error)
	ListTables(ctx context.Context, tx sql.Transaction, sn sql.SchemaName) ([]sql.Identifier,
		error)

	Begin(sesid uint64) sql.Transaction
}

type Engine struct {
	mutex            sync.RWMutex
	st               store
	systemInfoTables map[sql.Identifier]sql.MakeVirtual
	metadataTables   map[sql.Identifier]sql.MakeVirtual
}

func NewEngine(st store) sql.Engine {
	e := &Engine{
		st:               st,
		systemInfoTables: map[sql.Identifier]sql.MakeVirtual{},
		metadataTables:   map[sql.Identifier]sql.MakeVirtual{},
	}

	e.CreateSystemInfoTable(sql.ID("config"), makeConfigTable)
	e.CreateSystemInfoTable(sql.DATABASES, e.makeDatabasesTable)
	e.CreateSystemInfoTable(sql.ID("identifiers"), makeIdentifiersTable)

	e.CreateMetadataTable(sql.COLUMNS, e.makeColumnsTable)
	e.CreateMetadataTable(sql.CONSTRAINTS, e.makeConstraintsTable)
	e.CreateMetadataTable(sql.SCHEMAS, e.makeSchemasTable)
	e.CreateMetadataTable(sql.TABLES, e.makeTablesTable)

	return e
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

	st, tt, err := e.st.LookupTable(ctx, tx, tn)
	if err != nil {
		return nil, nil, err
	}
	return makeTable(tn, st, tt)
}

func (e *Engine) CreateTable(ctx context.Context, tx sql.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, cons []sql.Constraint,
	ifNotExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}

	var primary []sql.ColumnKey
	for _, con := range cons {
		if con.Type == sql.PrimaryConstraint {
			primary = con.Key
			break
		}
	}

	if len(primary) == 0 {
		rowID := sql.ID("rowid")

		for _, col := range cols {
			if col == rowID {
				return fmt.Errorf(
					"engine: unable to add %s column for table %s missing primary key", rowID, tn)
			}
		}

		primary = []sql.ColumnKey{
			sql.MakeColumnKey(len(cols), false),
		}
		cols = append(cols, rowID)

		dflt, err := expr.CompileExpr(&expr.Call{Name: sql.ID("unique_rowid")})
		if err != nil {
			panic(fmt.Sprintf("unable to compile default for rowid: %s", err))
		}
		colTypes = append(colTypes, sql.ColumnType{
			Type:    sql.IntegerType,
			Size:    8,
			NotNull: true,
			Default: dflt,
		})
	}

	tt := MakeTableType(cols, colTypes, primary)
	for _, con := range cons {
		if con.Type == sql.CheckConstraint {
			tt.checks = append(tt.checks,
				checkConstraint{
					name:      con.Name,
					check:     con.Check,
					checkExpr: con.CheckExpr,
				})
		} else if con.Type == sql.UniqueConstraint {
			it := e.st.MakeIndexType(tt, con.Name, con.Key, true)
			tt.indexes = append(tt.indexes, it)
		} else if con.Type == sql.PrimaryConstraint {
			// Used above; remove from constraints.
		} else {
			// sql.DefaultConstraint
			// sql.NotNullConstraint

			tt.constraints = append(tt.constraints,
				constraint{
					name:   con.Name,
					typ:    con.Type,
					colNum: con.ColNum,
				})
		}
	}

	for _, ck := range primary {
		col := ck.Column()
		if !colTypes[col].NotNull {
			colTypes[col].NotNull = true
			tt.constraints = append(tt.constraints,
				constraint{
					typ:    sql.NotNullConstraint,
					colNum: col,
				})
		}
	}

	return e.st.CreateTable(ctx, tx, tn, tt, ifNotExists)
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

func (e *Engine) AddForeignKey(ctx context.Context, tx sql.Transaction, con sql.Identifier,
	fktn sql.TableName, fkCols []int, rtn sql.TableName, ridx sql.Identifier) error {

	// XXX
	return nil
}

func (e *Engine) CreateIndex(ctx context.Context, tx sql.Transaction, idxname sql.Identifier,
	tn sql.TableName, unique bool, key []sql.ColumnKey, ifNotExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}

	_, tt, err := e.st.LookupTable(ctx, tx, tn)
	if err != nil {
		return err
	}

	for _, it := range tt.indexes {
		if it.Name == idxname {
			if ifNotExists {
				return nil
			}
			return fmt.Errorf("engine: table %s: index %s already exists", tn, idxname)
		}
	}

	tt.ver += 1
	it := e.st.MakeIndexType(tt, idxname, key, unique)
	tt.indexes = append(tt.indexes, it)
	return e.st.AddIndex(ctx, tx, tn, tt, it)
}

func (e *Engine) DropIndex(ctx context.Context, tx sql.Transaction, idxname sql.Identifier,
	tn sql.TableName, ifExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("engine: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("engine: schema %s may not be modified", tn.Schema)
	}

	_, tt, err := e.st.LookupTable(ctx, tx, tn)
	if err != nil {
		return err
	}

	var rdx int
	indexes := make([]sql.IndexType, 0, len(tt.indexes))
	for idx, it := range tt.indexes {
		if it.Name != idxname {
			indexes = append(indexes, it)
		} else {
			rdx = idx
		}
	}

	if len(indexes) == len(tt.indexes) {
		if ifExists {
			return nil
		}
		return fmt.Errorf("engine: table %s: index %s not found", tn, idxname)
	}

	tt.ver += 1
	tt.indexes = indexes

	return e.st.RemoveIndex(ctx, tx, tn, tt, rdx)
}

func (e *Engine) Begin(sesid uint64) sql.Transaction {
	return e.st.Begin(sesid)
}

func (e *Engine) ListDatabases(ctx context.Context, tx sql.Transaction) ([]sql.Identifier, error) {
	return e.st.ListDatabases(ctx, tx)
}
