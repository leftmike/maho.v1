package virtual

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type Engine interface {
	engine.Engine
	ListDatabases(ctx context.Context, tx engine.Transaction) ([]sql.Identifier, error)
	ListTables(ctx context.Context, tx engine.Transaction, name sql.Identifier) ([]sql.Identifier,
		error)
}

type tableMap map[sql.Identifier]engine.MakeVirtual

type virtualEngine struct {
	mutex        sync.RWMutex
	e            Engine
	infoTables   tableMap
	systemTables tableMap
}

func NewEngine(e Engine) engine.Engine {
	ve := &virtualEngine{
		e:            e,
		infoTables:   tableMap{},
		systemTables: tableMap{},
	}

	ve.CreateInfoTable(sql.ID("db$tables"), ve.makeTablesTable)
	ve.CreateInfoTable(sql.ID("db$columns"), ve.makeColumnsTable)

	ve.CreateSystemTable(sql.ID("databases"), ve.makeDatabasesTable)
	ve.CreateSystemTable(sql.ID("identifiers"), makeIdentifiersTable)
	ve.CreateSystemTable(sql.ID("config"), makeConfigTable)

	return ve
}

func (ve *virtualEngine) CreateSystemTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	ve.mutex.Lock()
	defer ve.mutex.Unlock()

	if _, ok := ve.systemTables[tblname]; ok {
		panic(fmt.Sprintf("system table already created: *.%s", tblname))
	}
	ve.systemTables[tblname] = maker
}

func (ve *virtualEngine) CreateInfoTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	ve.mutex.Lock()
	defer ve.mutex.Unlock()

	if _, ok := ve.infoTables[tblname]; ok {
		panic(fmt.Sprintf("information table already created: *.%s", tblname))
	}
	ve.infoTables[tblname] = maker
}

func (ve *virtualEngine) CreateDatabase(name sql.Identifier, options engine.Options) error {
	if name == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s already exists", name)
	}
	return ve.e.CreateDatabase(name, options)
}

func (ve *virtualEngine) DropDatabase(name sql.Identifier, exists bool,
	options engine.Options) error {

	if name == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be dropped", name)
	}
	return ve.e.DropDatabase(name, exists, options)
}

func (ve *virtualEngine) LookupTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier) (engine.Table, error) {

	ve.mutex.RLock()
	defer ve.mutex.RUnlock()

	if dbname == sql.SYSTEM {
		if maker, ok := ve.systemTables[tblname]; ok {
			return maker(ctx, tx, dbname, tblname)
		}
		if maker, ok := ve.infoTables[tblname]; ok {
			return maker(ctx, tx, dbname, tblname)
		}
		return nil, fmt.Errorf("virtual: table %s.%s not found", dbname, tblname)
	}
	if maker, ok := ve.infoTables[tblname]; ok {
		return maker(ctx, tx, dbname, tblname)
	}
	return ve.e.LookupTable(ctx, tx, dbname, tblname)
}

func (ve *virtualEngine) checkCreateDrop(dbname, tblname sql.Identifier) error {
	if dbname == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", dbname)
	}

	ve.mutex.RLock()
	defer ve.mutex.RUnlock()
	if _, ok := ve.infoTables[tblname]; ok {
		return fmt.Errorf("virtual: table %s may not be modified", tblname)
	}
	return nil
}

func (ve *virtualEngine) CreateTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier, cols []sql.Identifier, colTypes []sql.ColumnType) error {

	err := ve.checkCreateDrop(dbname, tblname)
	if err != nil {
		return err
	}
	return ve.e.CreateTable(ctx, tx, dbname, tblname, cols, colTypes)
}

func (ve *virtualEngine) DropTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier, exists bool) error {

	err := ve.checkCreateDrop(dbname, tblname)
	if err != nil {
		return err
	}
	return ve.e.DropTable(ctx, tx, dbname, tblname, exists)
}

func (ve *virtualEngine) Begin(sid uint64) engine.Transaction {
	return ve.e.Begin(sid)
}

func (ve *virtualEngine) IsTransactional() bool {
	return ve.e.IsTransactional()
}

func MakeTable(name string, cols []sql.Identifier, colTypes []sql.ColumnType,
	values [][]sql.Value) engine.Table {

	return &virtualTable{
		name:     name,
		cols:     cols,
		colTypes: colTypes,
		values:   values,
	}
}

type virtualTable struct {
	name     string
	cols     []sql.Identifier
	colTypes []sql.ColumnType
	values   [][]sql.Value
}

type virtualRows struct {
	name    string
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
}

func (vt *virtualTable) Columns(ctx context.Context) []sql.Identifier {
	return vt.cols
}

func (vt *virtualTable) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return vt.colTypes
}

func (vt *virtualTable) Rows(ctx context.Context) (engine.Rows, error) {
	return &virtualRows{name: vt.name, columns: vt.cols, rows: vt.values}, nil
}

func (vt *virtualTable) Insert(ctx context.Context, row []sql.Value) error {
	return fmt.Errorf("virtual: table %s can not be modified", vt.name)
}

func (vr *virtualRows) Columns() []sql.Identifier {
	return vr.columns
}

func (vr *virtualRows) Close() error {
	vr.index = len(vr.rows)
	return nil
}

func (vr *virtualRows) Next(ctx context.Context, dest []sql.Value) error {
	for vr.index < len(vr.rows) {
		if vr.rows[vr.index] != nil {
			copy(dest, vr.rows[vr.index])
			vr.index += 1
			return nil
		}
		vr.index += 1
	}

	return io.EOF
}

func (vr *virtualRows) Delete(ctx context.Context) error {
	return fmt.Errorf("virtual: table %s can not be modified", vr.name)
}

func (vr *virtualRows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	return fmt.Errorf("virtual: table %s can not be modified", vr.name)
}

func (ve *virtualEngine) makeTablesTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier) (engine.Table, error) {

	ve.mutex.RLock()
	defer ve.mutex.RUnlock()

	values := [][]sql.Value{}

	if dbname == sql.SYSTEM {
		for tblname := range ve.systemTables {
			values = append(values, []sql.Value{
				sql.StringValue(tblname.String()),
				sql.StringValue("virtual"),
			})
		}
	} else {
		tblnames, err := ve.e.ListTables(ctx, tx, dbname)
		if err != nil {
			return nil, err
		}

		for _, tblname := range tblnames {
			values = append(values, []sql.Value{
				sql.StringValue(tblname.String()),
				sql.StringValue("physical"),
			})
		}
	}

	for tblname := range ve.infoTables {
		values = append(values, []sql.Value{
			sql.StringValue(tblname.String()),
			sql.StringValue("virtual"),
		})
	}

	return MakeTable(fmt.Sprintf("%s.%s", dbname, tblname),
		[]sql.Identifier{sql.ID("table"), sql.ID("type")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType}, values), nil
}

var (
	columnsColumns = []sql.Identifier{sql.ID("table"), sql.ID("column"), sql.ID("type"),
		sql.ID("size"), sql.ID("fixed"), sql.ID("binary"), sql.ID("not_null"), sql.ID("default")}
	columnsColumnTypes = []sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType,
		sql.Int32ColType, sql.BoolColType, sql.BoolColType, sql.BoolColType, sql.NullStringColType}
)

func appendColumns(values [][]sql.Value, tblname sql.Identifier, cols []sql.Identifier,
	colTypes []sql.ColumnType) [][]sql.Value {

	for i, ct := range colTypes {
		var def sql.Value
		if ct.Default != nil {
			def = sql.StringValue(ct.Default.String())
		}
		values = append(values,
			[]sql.Value{
				sql.StringValue(tblname.String()),
				sql.StringValue(cols[i].String()),
				sql.StringValue(ct.Type.String()),
				sql.Int64Value(ct.Size),
				sql.BoolValue(ct.Fixed),
				sql.BoolValue(ct.Binary),
				sql.BoolValue(ct.NotNull),
				def,
			})
	}
	return values
}

func (ve *virtualEngine) makeColumnsTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier) (engine.Table, error) {

	ve.mutex.RLock()
	defer ve.mutex.RUnlock()

	values := [][]sql.Value{}

	if dbname == sql.SYSTEM {
		for tblname := range ve.systemTables {
			tbl, err := ve.LookupTable(ctx, tx, dbname, tblname)
			if err != nil {
				return nil, err
			}
			values = appendColumns(values, tblname, tbl.Columns(ctx), tbl.ColumnTypes(ctx))
		}
	} else {
		tblnames, err := ve.e.ListTables(ctx, tx, dbname)
		if err != nil {
			return nil, err
		}

		for _, tblname := range tblnames {
			tbl, err := ve.e.LookupTable(ctx, tx, dbname, tblname)
			if err != nil {
				return nil, err
			}
			values = appendColumns(values, tblname, tbl.Columns(ctx), tbl.ColumnTypes(ctx))
		}
	}

	for tblname := range ve.infoTables {
		if tblname == sql.ID("db$columns") {
			values = appendColumns(values, sql.ID("db$columns"), columnsColumns, columnsColumnTypes)
		} else {
			tbl, err := ve.LookupTable(ctx, tx, dbname, tblname)
			if err != nil {
				return nil, err
			}
			values = appendColumns(values, tblname, tbl.Columns(ctx), tbl.ColumnTypes(ctx))
		}
	}

	return MakeTable(fmt.Sprintf("%s.%s", dbname, tblname), columnsColumns, columnsColumnTypes,
		values), nil
}

func (ve *virtualEngine) makeDatabasesTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier) (engine.Table, error) {

	dbnames, err := ve.e.ListDatabases(ctx, tx)
	if err != nil {
		return nil, err
	}

	values := [][]sql.Value{
		[]sql.Value{
			sql.StringValue(sql.SYSTEM.String()),
		},
	}

	for _, dbname := range dbnames {
		values = append(values, []sql.Value{
			sql.StringValue(dbname.String()),
		})
	}
	return MakeTable(fmt.Sprintf("%s.%s", dbname, tblname),
		[]sql.Identifier{sql.ID("database")}, []sql.ColumnType{sql.IdColType}, values), nil

}

func makeIdentifiersTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier) (engine.Table, error) {

	values := [][]sql.Value{}

	for id, n := range sql.Names {
		values = append(values,
			[]sql.Value{
				sql.StringValue(n),
				sql.Int64Value(id),
				sql.BoolValue(id.IsReserved()),
			})
	}

	return MakeTable(fmt.Sprintf("%s.%s", dbname, tblname),
		[]sql.Identifier{sql.ID("name"), sql.ID("id"), sql.ID("reserved")},
		[]sql.ColumnType{sql.IdColType, sql.Int32ColType, sql.BoolColType}, values), nil
}

func makeConfigTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier) (engine.Table, error) {

	values := [][]sql.Value{}

	for _, v := range config.Vars() {
		values = append(values,
			[]sql.Value{
				sql.StringValue(v.Name()),
				sql.StringValue(v.By()),
				sql.StringValue(v.Val()),
			})
	}

	return MakeTable(fmt.Sprintf("%s.%s", dbname, tblname),
		[]sql.Identifier{sql.ID("name"), sql.ID("by"), sql.ID("value")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.StringColType}, values), nil
}
