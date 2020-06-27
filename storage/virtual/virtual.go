package virtual

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

type virtualEngine struct {
	mutex            sync.RWMutex
	e                storage.Store
	systemInfoTables map[sql.Identifier]storage.MakeVirtual
	metadataTables   map[sql.Identifier]storage.MakeVirtual
}

func NewStore(e storage.Store) storage.Store {
	ve := &virtualEngine{
		e:                e,
		systemInfoTables: map[sql.Identifier]storage.MakeVirtual{},
		metadataTables:   map[sql.Identifier]storage.MakeVirtual{},
	}

	ve.CreateSystemInfoTable(sql.ID("config"), makeConfigTable)
	ve.CreateSystemInfoTable(sql.DATABASES, ve.makeDatabasesTable)
	ve.CreateSystemInfoTable(sql.ID("identifiers"), makeIdentifiersTable)

	ve.CreateMetadataTable(sql.COLUMNS, ve.makeColumnsTable)
	ve.CreateMetadataTable(sql.SCHEMAS, ve.makeSchemasTable)
	ve.CreateMetadataTable(sql.TABLES, ve.makeTablesTable)

	return ve
}

func (ve *virtualEngine) CreateSystemInfoTable(tblname sql.Identifier, maker storage.MakeVirtual) {
	ve.mutex.Lock()
	defer ve.mutex.Unlock()

	if _, ok := ve.systemInfoTables[tblname]; ok {
		panic(fmt.Sprintf("system info table already created: %s", tblname))
	}
	ve.systemInfoTables[tblname] = maker
}

func (ve *virtualEngine) CreateMetadataTable(tblname sql.Identifier, maker storage.MakeVirtual) {
	ve.mutex.Lock()
	defer ve.mutex.Unlock()

	if _, ok := ve.metadataTables[tblname]; ok {
		panic(fmt.Sprintf("metadata table already created: %s", tblname))
	}
	ve.metadataTables[tblname] = maker
}

func (ve *virtualEngine) CreateDatabase(dbname sql.Identifier, options storage.Options) error {
	if dbname == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s already exists", dbname)
	}
	return ve.e.CreateDatabase(dbname, options)
}

func (ve *virtualEngine) DropDatabase(dbname sql.Identifier, ifExists bool,
	options storage.Options) error {

	if dbname == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be dropped", dbname)
	}
	return ve.e.DropDatabase(dbname, ifExists, options)
}

func (ve *virtualEngine) CreateSchema(ctx context.Context, tx storage.Transaction,
	sn sql.SchemaName) error {

	if sn.Database == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", sn.Database)
	}
	if sn.Schema == sql.METADATA {
		return fmt.Errorf("virtual: schema %s already exists", sn)
	}
	return ve.e.CreateSchema(ctx, tx, sn)
}

func (ve *virtualEngine) DropSchema(ctx context.Context, tx storage.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	if sn.Database == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", sn.Database)
	}
	if sn.Schema == sql.METADATA {
		return fmt.Errorf("virtual: schema %s may not be dropped", sn)
	}
	return ve.e.DropSchema(ctx, tx, sn, ifExists)
}

func (ve *virtualEngine) LookupTable(ctx context.Context, tx storage.Transaction,
	tn sql.TableName) (storage.Table, error) {

	ve.mutex.RLock()
	defer ve.mutex.RUnlock()

	if tn.Schema == sql.METADATA {
		if maker, ok := ve.metadataTables[tn.Table]; ok {
			return maker(ctx, tx, tn)
		}
		return nil, fmt.Errorf("virtual: table %s not found", tn)
	} else if tn.Database == sql.SYSTEM && tn.Schema == sql.INFO {
		if maker, ok := ve.systemInfoTables[tn.Table]; ok {
			return maker(ctx, tx, tn)
		}
		return nil, fmt.Errorf("virtual: table %s not found", tn)
	}

	return ve.e.LookupTable(ctx, tx, tn)
}

func (ve *virtualEngine) CreateTable(ctx context.Context, tx storage.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []storage.ColumnKey,
	ifNotExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("virtual: schema %s may not be modified", tn.Schema)
	}
	return ve.e.CreateTable(ctx, tx, tn, cols, colTypes, primary, ifNotExists)
}

func (ve *virtualEngine) DropTable(ctx context.Context, tx storage.Transaction, tn sql.TableName,
	ifExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("virtual: schema %s may not be modified", tn.Schema)
	}
	return ve.e.DropTable(ctx, tx, tn, ifExists)
}

func (ve *virtualEngine) CreateIndex(ctx context.Context, tx storage.Transaction,
	idxname sql.Identifier, tn sql.TableName, unique bool, keys []storage.ColumnKey,
	ifNotExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("virtual: schema %s may not be modified", tn.Schema)
	}
	return ve.e.CreateIndex(ctx, tx, idxname, tn, unique, keys, ifNotExists)
}

func (ve *virtualEngine) DropIndex(ctx context.Context, tx storage.Transaction,
	idxname sql.Identifier, tn sql.TableName, ifExists bool) error {

	if tn.Database == sql.SYSTEM {
		return fmt.Errorf("virtual: database %s may not be modified", tn.Database)
	}
	if tn.Schema == sql.METADATA {
		return fmt.Errorf("virtual: schema %s may not be modified", tn.Schema)
	}
	return ve.e.DropIndex(ctx, tx, idxname, tn, ifExists)
}

func (ve *virtualEngine) Begin(sesid uint64) storage.Transaction {
	return ve.e.Begin(sesid)
}

func (ve *virtualEngine) ListDatabases(ctx context.Context,
	tx storage.Transaction) ([]sql.Identifier, error) {

	return ve.e.ListDatabases(ctx, tx)
}

func (ve *virtualEngine) ListSchemas(ctx context.Context, tx storage.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	return ve.e.ListSchemas(ctx, tx, dbname)
}

func (ve *virtualEngine) ListTables(ctx context.Context, tx storage.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	return ve.e.ListTables(ctx, tx, sn)
}

func MakeTable(tn sql.TableName, cols []sql.Identifier, colTypes []sql.ColumnType,
	values [][]sql.Value) storage.Table {

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

func (vt *virtualTable) PrimaryKey(ctx context.Context) []storage.ColumnKey {
	return nil
}

func (vt *virtualTable) Rows(ctx context.Context, minRow, maxRow []sql.Value) (storage.Rows, error) {
	if minRow != nil || maxRow != nil {
		panic("virtual: not implemented: minRow != nil || maxRow != nil")
	}
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

func (ve *virtualEngine) listSchemas(ctx context.Context, tx storage.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	if dbname == sql.SYSTEM {
		dbnames, err := ve.e.ListDatabases(ctx, tx)
		if err != nil {
			return nil, err
		}

		var found bool
		for _, dbname := range dbnames {
			if dbname == sql.SYSTEM {
				found = true
			}
		}
		if !found {
			return []sql.Identifier{sql.METADATA, sql.INFO}, nil
		}
	}

	scnames, err := ve.e.ListSchemas(ctx, tx, dbname)
	if err != nil {
		return nil, err
	}
	scnames = append(scnames, sql.METADATA)
	if dbname == sql.SYSTEM {
		scnames = append(scnames, sql.INFO)
	}
	return scnames, nil
}

func (ve *virtualEngine) makeSchemasTable(ctx context.Context, tx storage.Transaction,
	tn sql.TableName) (storage.Table, error) {

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
		[]sql.Identifier{sql.ID("database_name"), sql.ID("schema_name")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType}, values), nil
}

func (ve *virtualEngine) listTables(ctx context.Context, tx storage.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	if sn.Schema == sql.METADATA {
		var tblnames []sql.Identifier
		for tblname := range ve.metadataTables {
			tblnames = append(tblnames, tblname)
		}
		return tblnames, nil
	} else if sn.Database == sql.SYSTEM && sn.Schema == sql.INFO {
		var tblnames []sql.Identifier
		for tblname := range ve.systemInfoTables {
			tblnames = append(tblnames, tblname)
		}
		return tblnames, nil
	}
	return ve.e.ListTables(ctx, tx, sn)
}

func (ve *virtualEngine) makeTablesTable(ctx context.Context, tx storage.Transaction,
	tn sql.TableName) (storage.Table, error) {

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
		[]sql.Identifier{sql.ID("database_name"), sql.ID("schema_name"), sql.ID("table_name")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType}, values), nil
}

var (
	columnsColumns = []sql.Identifier{sql.ID("database_name"), sql.ID("schema_name"),
		sql.ID("table_name"), sql.ID("column_name"), sql.ID("position"), sql.ID("default"),
		sql.ID("is_nullable"), sql.ID("data_type"),
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

func (ve *virtualEngine) makeColumnsTable(ctx context.Context, tx storage.Transaction,
	tn sql.TableName) (storage.Table, error) {

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
			if scname == sql.METADATA && tblname == sql.ID("columns") {
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

func (ve *virtualEngine) makeDatabasesTable(ctx context.Context, tx storage.Transaction,
	tn sql.TableName) (storage.Table, error) {

	dbnames, err := ve.e.ListDatabases(ctx, tx)
	if err != nil {
		return nil, err
	}

	var found bool
	var values [][]sql.Value
	for _, dbname := range dbnames {
		values = append(values, []sql.Value{
			sql.StringValue(dbname.String()),
		})
		if dbname == sql.SYSTEM {
			found = true
		}
	}
	if !found {
		values = append(values,
			[]sql.Value{
				sql.StringValue(sql.SYSTEM.String()),
			})
	}

	return MakeTable(tn, []sql.Identifier{sql.ID("database")}, []sql.ColumnType{sql.IdColType},
		values), nil

}

func makeIdentifiersTable(ctx context.Context, tx storage.Transaction,
	tn sql.TableName) (storage.Table, error) {

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

func makeConfigTable(ctx context.Context, tx storage.Transaction, tn sql.TableName) (storage.Table,
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
