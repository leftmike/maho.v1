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

type virtualEngine struct {
	mutex        sync.RWMutex
	e            engine.Engine
	systemTables map[sql.TableName]engine.MakeVirtual
	infoTables   map[sql.Identifier]engine.MakeVirtual
}

func NewEngine(e engine.Engine) engine.Engine {
	ve := &virtualEngine{
		e:            e,
		systemTables: map[sql.TableName]engine.MakeVirtual{},
		infoTables:   map[sql.Identifier]engine.MakeVirtual{},
	}

	ve.CreateSystemTable(sql.ID("config"), makeConfigTable)
	ve.CreateSystemTable(sql.DATABASES, ve.makeDatabasesTable)
	ve.CreateSystemTable(sql.ID("identifiers"), makeIdentifiersTable)

	ve.CreateInfoTable(sql.COLUMNS, ve.makeColumnsTable)
	ve.CreateInfoTable(sql.SCHEMATA, ve.makeSchemataTable)
	ve.CreateInfoTable(sql.TABLES, ve.makeTablesTable)

	return ve
}

func (ve *virtualEngine) CreateSystemTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	ve.mutex.Lock()
	defer ve.mutex.Unlock()

	tn := sql.TableName{sql.SYSTEM, sql.PUBLIC, tblname}
	if _, ok := ve.systemTables[tn]; ok {
		panic(fmt.Sprintf("system table already created: %s", tn))
	}
	ve.systemTables[tn] = maker
}

func (ve *virtualEngine) CreateInfoTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	ve.mutex.Lock()
	defer ve.mutex.Unlock()

	if _, ok := ve.infoTables[tblname]; ok {
		panic(fmt.Sprintf("information table already created: %s", tblname))
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

	if sn.Database == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", sn.Database)
	}
	if sn.Schema == sql.INFORMATION_SCHEMA {
		return fmt.Errorf("virtual: schema %s already exists", sn)
	}
	return ve.e.CreateSchema(ctx, tx, sn)
}

func (ve *virtualEngine) DropSchema(ctx context.Context, tx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	if sn.Database == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", sn.Database)
	}
	if sn.Schema == sql.INFORMATION_SCHEMA {
		return fmt.Errorf("virtual: schema %s may not be dropped", sn)
	}
	return ve.e.DropSchema(ctx, tx, sn, ifExists)
}

func (ve *virtualEngine) LookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	ve.mutex.RLock()
	defer ve.mutex.RUnlock()

	if tn.Database == sql.SYSTEM {
		if maker, ok := ve.systemTables[tn]; ok {
			return maker(ctx, tx, tn)
		}
		if maker, ok := ve.infoTables[tn.Table]; ok && tn.Schema == sql.INFORMATION_SCHEMA {
			return maker(ctx, tx, tn)
		}
		return nil, fmt.Errorf("virtual: table %s not found", tn)
	}
	if maker, ok := ve.infoTables[tn.Table]; ok && tn.Schema == sql.INFORMATION_SCHEMA {
		return maker(ctx, tx, tn)
	}
	return ve.e.LookupTable(ctx, tx, tn)
}

func (ve *virtualEngine) CreateTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.INFORMATION_SCHEMA {
		return fmt.Errorf("virtual: schema %s may not be modified", tn.Schema)
	}
	return ve.e.CreateTable(ctx, tx, tn, cols, colTypes, primary, ifNotExists)
}

func (ve *virtualEngine) DropTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.INFORMATION_SCHEMA {
		return fmt.Errorf("virtual: schema %s may not be modified", tn.Schema)
	}
	return ve.e.DropTable(ctx, tx, tn, ifExists)
}

func (ve *virtualEngine) CreateIndex(ctx context.Context, tx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, unique bool, keys []engine.ColumnKey,
	ifNotExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.INFORMATION_SCHEMA {
		return fmt.Errorf("virtual: schema %s may not be modified", tn.Schema)
	}
	return ve.e.CreateIndex(ctx, tx, idxname, tn, unique, keys, ifNotExists)
}

func (ve *virtualEngine) DropIndex(ctx context.Context, tx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, ifExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.INFORMATION_SCHEMA {
		return fmt.Errorf("virtual: schema %s may not be modified", tn.Schema)
	}
	return ve.e.DropIndex(ctx, tx, idxname, tn, ifExists)
}

func (ve *virtualEngine) Begin(sesid uint64) engine.Transaction {
	return ve.e.Begin(sesid)
}

func (ve *virtualEngine) IsTransactional() bool {
	return ve.e.IsTransactional()
}

func (ve *virtualEngine) ListDatabases(ctx context.Context,
	tx engine.Transaction) ([]sql.Identifier, error) {

	return ve.e.ListDatabases(ctx, tx)
}

func (ve *virtualEngine) ListSchemas(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	return ve.e.ListSchemas(ctx, tx, dbname)
}

func (ve *virtualEngine) ListTables(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	return ve.e.ListTables(ctx, tx, sn)
}

func MakeTable(tn sql.TableName, cols []sql.Identifier, colTypes []sql.ColumnType,
	values [][]sql.Value) engine.Table {

	return &virtualTable{
		tn:       tn,
		cols:     cols,
		colTypes: colTypes,
		values:   values,
	}
}

type virtualTable struct {
	tn       sql.TableName
	cols     []sql.Identifier
	colTypes []sql.ColumnType
	values   [][]sql.Value
}

type virtualRows struct {
	tn      sql.TableName
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

func (vt *virtualTable) PrimaryKey(ctx context.Context) []engine.ColumnKey {
	return nil
}

func (vt *virtualTable) Seek(ctx context.Context, row []sql.Value) (engine.Rows, error) {
	return vt.Rows(ctx)
}

func (vt *virtualTable) Rows(ctx context.Context) (engine.Rows, error) {
	return &virtualRows{tn: vt.tn, columns: vt.cols, rows: vt.values}, nil
}

func (vt *virtualTable) Insert(ctx context.Context, row []sql.Value) error {
	return fmt.Errorf("virtual: table %s can not be modified", vt.tn)
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
	return fmt.Errorf("virtual: table %s can not be modified", vr.tn)
}

func (vr *virtualRows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	return fmt.Errorf("virtual: table %s can not be modified", vr.tn)
}

func (ve *virtualEngine) listSchemas(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	if dbname == sql.SYSTEM {
		return []sql.Identifier{sql.INFORMATION_SCHEMA, sql.PUBLIC}, nil
	}

	scnames, err := ve.e.ListSchemas(ctx, tx, dbname)
	if err != nil {
		return nil, err
	}
	return append(scnames, sql.INFORMATION_SCHEMA), nil
}

func (ve *virtualEngine) makeSchemataTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	ve.mutex.RLock()
	defer ve.mutex.RUnlock()

	values := [][]sql.Value{}

	scnames, err := ve.listSchemas(ctx, tx, tn.Database)
	if err != nil {
		return nil, err
	}

	for _, scname := range scnames {
		values = append(values, []sql.Value{
			sql.StringValue(tn.Database.String()),
			sql.StringValue(scname.String()),
		})
	}

	return MakeTable(tn,
		[]sql.Identifier{sql.ID("catalog_name"), sql.ID("schema_name")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType}, values), nil
}

func (ve *virtualEngine) listTables(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	var tblnames []sql.Identifier

	if sn.Schema == sql.INFORMATION_SCHEMA {
		for tblname := range ve.infoTables {
			tblnames = append(tblnames, tblname)
		}
	} else if sn.Database == sql.SYSTEM {
		for tn := range ve.systemTables {
			tblnames = append(tblnames, tn.Table)
		}
	} else {
		return ve.e.ListTables(ctx, tx, sn)
	}

	return tblnames, nil
}

func (ve *virtualEngine) makeTablesTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	ve.mutex.RLock()
	defer ve.mutex.RUnlock()

	values := [][]sql.Value{}

	scnames, err := ve.listSchemas(ctx, tx, tn.Database)
	if err != nil {
		return nil, err
	}

	for _, scname := range scnames {
		tblnames, err := ve.listTables(ctx, tx, sql.SchemaName{tn.Database, scname})
		if err != nil {
			return nil, err
		}

		for _, tblname := range tblnames {
			values = append(values, []sql.Value{
				sql.StringValue(tn.Database.String()),
				sql.StringValue(scname.String()),
				sql.StringValue(tblname.String()),
			})
		}
	}

	return MakeTable(tn,
		[]sql.Identifier{sql.ID("table_catalog"), sql.ID("table_schema"), sql.ID("table_name")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType}, values), nil
}

var (
	columnsColumns = []sql.Identifier{sql.ID("table_catalog"), sql.ID("table_schema"),
		sql.ID("table_name"), sql.ID("column_name"), sql.ID("ordinal_position"),
		sql.ID("column_default"), sql.ID("is_nullable"), sql.ID("data_type"),
	}
	columnsColumnTypes = []sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType,
		sql.IdColType, sql.Int32ColType, sql.NullStringColType, sql.StringColType,
		sql.IdColType,
	}
)

func appendColumns(values [][]sql.Value, tn sql.TableName, cols []sql.Identifier,
	colTypes []sql.ColumnType) [][]sql.Value {

	for i, ct := range colTypes {
		var def sql.Value
		if ct.Default != nil {
			def = sql.StringValue(ct.Default.String())
		}
		isNullable := "yes"
		if ct.NotNull {
			isNullable = "no"
		}
		values = append(values,
			[]sql.Value{
				sql.StringValue(tn.Database.String()),
				sql.StringValue(tn.Schema.String()),
				sql.StringValue(tn.Table.String()),
				sql.StringValue(cols[i].String()),
				sql.Int64Value(i + 1),
				def,
				sql.StringValue(isNullable),
				sql.StringValue(ct.Type.String()),
			})
	}
	return values
}

func (ve *virtualEngine) makeColumnsTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	ve.mutex.RLock()
	defer ve.mutex.RUnlock()

	values := [][]sql.Value{}

	scnames, err := ve.listSchemas(ctx, tx, tn.Database)
	if err != nil {
		return nil, err
	}

	for _, scname := range scnames {
		tblnames, err := ve.listTables(ctx, tx, sql.SchemaName{tn.Database, scname})
		if err != nil {
			return nil, err
		}

		for _, tblname := range tblnames {
			ttn := sql.TableName{tn.Database, scname, tblname}
			if scname == sql.INFORMATION_SCHEMA && tblname == sql.ID("columns") {
				values = appendColumns(values, ttn, columnsColumns, columnsColumnTypes)
			} else {
				tbl, err := ve.LookupTable(ctx, tx, ttn)
				if err != nil {
					return nil, err
				}
				values = appendColumns(values, ttn, tbl.Columns(ctx), tbl.ColumnTypes(ctx))
			}
		}
	}

	return MakeTable(tn, columnsColumns, columnsColumnTypes, values), nil
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
	return MakeTable(tn, []sql.Identifier{sql.ID("database")}, []sql.ColumnType{sql.IdColType},
		values), nil

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

	return MakeTable(tn,
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

	return MakeTable(tn,
		[]sql.Identifier{sql.ID("name"), sql.ID("by"), sql.ID("value")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.StringColType}, values), nil
}
