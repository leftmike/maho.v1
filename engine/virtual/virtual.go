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
	ListTables(ctx context.Context, tx engine.Transaction, dbname sql.Identifier) ([]sql.Identifier,
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

func (ve *virtualEngine) CreateDatabase(dbname sql.Identifier, options engine.Options) error {
	if dbname == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s already exists", dbname)
	}
	return ve.e.CreateDatabase(dbname, options)
}

func (ve *virtualEngine) DropDatabase(dbname sql.Identifier, ifExists bool,
	options engine.Options) error {

	if dbname == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be dropped", dbname)
	}
	return ve.e.DropDatabase(dbname, ifExists, options)
}

func (ve *virtualEngine) CreateSchema(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) error {

	return ve.e.CreateSchema(ctx, tx, sn)
}

func (ve *virtualEngine) DropSchema(ctx context.Context, tx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	return ve.e.DropSchema(ctx, tx, sn, ifExists)
}

func (ve *virtualEngine) LookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	ve.mutex.RLock()
	defer ve.mutex.RUnlock()

	if tn.Database == sql.SYSTEM {
		if maker, ok := ve.systemTables[tn.Table]; ok {
			return maker(ctx, tx, tn)
		}
		if maker, ok := ve.infoTables[tn.Table]; ok {
			return maker(ctx, tx, tn)
		}
		return nil, fmt.Errorf("virtual: table %s not found", tn)
	}
	if maker, ok := ve.infoTables[tn.Table]; ok {
		return maker(ctx, tx, tn)
	}
	return ve.e.LookupTable(ctx, tx, tn)
}

func (ve *virtualEngine) checkCreateDrop(tn sql.TableName) error {
	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", tn.Database)
	}

	ve.mutex.RLock()
	defer ve.mutex.RUnlock()
	if _, ok := ve.infoTables[tn.Table]; ok {
		return fmt.Errorf("virtual: table %s may not be modified", tn)
	}
	return nil
}

func (ve *virtualEngine) CreateTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType) error {

	err := ve.checkCreateDrop(tn)
	if err != nil {
		return err
	}
	return ve.e.CreateTable(ctx, tx, tn, cols, colTypes)
}

func (ve *virtualEngine) DropTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	err := ve.checkCreateDrop(tn)
	if err != nil {
		return err
	}
	return ve.e.DropTable(ctx, tx, tn, ifExists)
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
	tn sql.TableName) (engine.Table, error) {

	ve.mutex.RLock()
	defer ve.mutex.RUnlock()

	values := [][]sql.Value{}

	if tn.Database == sql.SYSTEM {
		for tblname := range ve.systemTables {
			values = append(values, []sql.Value{
				sql.StringValue(tblname.String()),
				sql.StringValue("virtual"),
			})
		}
	} else {
		tblnames, err := ve.e.ListTables(ctx, tx, tn.Database)
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

	return MakeTable(tn.String(),
		[]sql.Identifier{sql.ID("table"), sql.ID("type")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType}, values), nil
}

var (
	columnsColumns = []sql.Identifier{sql.ID("table"), sql.ID("column"), sql.ID("type"),
		sql.ID("size"), sql.ID("fixed"), sql.ID("binary"), sql.ID("not_null"), sql.ID("default")}
	columnsColumnTypes = []sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType,
		sql.Int32ColType, sql.BoolColType, sql.BoolColType, sql.BoolColType, sql.NullStringColType}
)

func appendColumns(values [][]sql.Value, tn sql.TableName, cols []sql.Identifier,
	colTypes []sql.ColumnType) [][]sql.Value {

	for i, ct := range colTypes {
		var def sql.Value
		if ct.Default != nil {
			def = sql.StringValue(ct.Default.String())
		}
		values = append(values,
			[]sql.Value{
				sql.StringValue(tn.Table.String()),
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
	tn sql.TableName) (engine.Table, error) {

	ve.mutex.RLock()
	defer ve.mutex.RUnlock()

	values := [][]sql.Value{}

	if tn.Database == sql.SYSTEM {
		for tblname := range ve.systemTables {
			ttn := sql.TableName{tn.Database, tn.Schema, tblname}
			tbl, err := ve.LookupTable(ctx, tx, ttn)
			if err != nil {
				return nil, err
			}
			values = appendColumns(values, ttn, tbl.Columns(ctx), tbl.ColumnTypes(ctx))
		}
	} else {
		tblnames, err := ve.e.ListTables(ctx, tx, tn.Database)
		if err != nil {
			return nil, err
		}

		for _, tblname := range tblnames {
			ttn := sql.TableName{tn.Database, tn.Schema, tblname}
			tbl, err := ve.e.LookupTable(ctx, tx, ttn)
			if err != nil {
				return nil, err
			}
			values = appendColumns(values, ttn, tbl.Columns(ctx), tbl.ColumnTypes(ctx))
		}
	}

	for tblname := range ve.infoTables {
		ttn := sql.TableName{tn.Database, tn.Schema, tblname}
		if tblname == sql.ID("db$columns") {
			values = appendColumns(values, ttn, columnsColumns, columnsColumnTypes)
		} else {
			tbl, err := ve.LookupTable(ctx, tx, ttn)
			if err != nil {
				return nil, err
			}
			values = appendColumns(values, ttn, tbl.Columns(ctx), tbl.ColumnTypes(ctx))
		}
	}

	return MakeTable(tn.String(), columnsColumns, columnsColumnTypes, values), nil
}

func (ve *virtualEngine) makeDatabasesTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

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
	return MakeTable(tn.String(), []sql.Identifier{sql.ID("database")},
		[]sql.ColumnType{sql.IdColType}, values), nil

}

func makeIdentifiersTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	values := [][]sql.Value{}

	for id, n := range sql.Names {
		values = append(values,
			[]sql.Value{
				sql.StringValue(n),
				sql.Int64Value(id),
				sql.BoolValue(id.IsReserved()),
			})
	}

	return MakeTable(tn.String(),
		[]sql.Identifier{sql.ID("name"), sql.ID("id"), sql.ID("reserved")},
		[]sql.ColumnType{sql.IdColType, sql.Int32ColType, sql.BoolColType}, values), nil
}

func makeConfigTable(ctx context.Context, tx engine.Transaction, tn sql.TableName) (engine.Table,
	error) {

	values := [][]sql.Value{}

	for _, v := range config.Vars() {
		values = append(values,
			[]sql.Value{
				sql.StringValue(v.Name()),
				sql.StringValue(v.By()),
				sql.StringValue(v.Val()),
			})
	}

	return MakeTable(tn.String(),
		[]sql.Identifier{sql.ID("name"), sql.ID("by"), sql.ID("value")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.StringColType}, values), nil
}
