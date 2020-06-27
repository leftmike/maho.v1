package engine

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

func MakeVirtualTable(tn sql.TableName, cols []sql.Identifier, colTypes []sql.ColumnType,
	values [][]sql.Value) Table {

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

func (vt *virtualTable) PrimaryKey(ctx context.Context) []sql.ColumnKey {
	return nil
}

func (vt *virtualTable) Rows(ctx context.Context, minRow, maxRow []sql.Value) (storage.Rows,
	error) {

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

func (e *engine) listSchemas(ctx context.Context, tx Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	if dbname == sql.SYSTEM {
		dbnames, err := e.st.ListDatabases(ctx, tx)
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

	scnames, err := e.st.ListSchemas(ctx, tx, dbname)
	if err != nil {
		return nil, err
	}
	scnames = append(scnames, sql.METADATA)
	if dbname == sql.SYSTEM {
		scnames = append(scnames, sql.INFO)
	}
	return scnames, nil
}

func (e *engine) makeSchemasTable(ctx context.Context, tx Transaction, tn sql.TableName) (Table,
	error) {

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	values := [][]sql.Value{}

	scnames, err := e.listSchemas(ctx, tx, tn.Database)
	if err != nil {
		return nil, err
	}

	for _, scname := range scnames {
		values = append(values, []sql.Value{
			sql.StringValue(tn.Database.String()),
			sql.StringValue(scname.String()),
		})
	}

	return MakeVirtualTable(tn,
		[]sql.Identifier{sql.ID("database_name"), sql.ID("schema_name")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType}, values), nil
}

func (e *engine) listTables(ctx context.Context, tx Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	if sn.Schema == sql.METADATA {
		var tblnames []sql.Identifier
		for tblname := range e.metadataTables {
			tblnames = append(tblnames, tblname)
		}
		return tblnames, nil
	} else if sn.Database == sql.SYSTEM && sn.Schema == sql.INFO {
		var tblnames []sql.Identifier
		for tblname := range e.systemInfoTables {
			tblnames = append(tblnames, tblname)
		}
		return tblnames, nil
	}
	return e.st.ListTables(ctx, tx, sn)
}

func (e *engine) makeTablesTable(ctx context.Context, tx Transaction, tn sql.TableName) (Table,
	error) {

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	values := [][]sql.Value{}

	scnames, err := e.listSchemas(ctx, tx, tn.Database)
	if err != nil {
		return nil, err
	}

	for _, scname := range scnames {
		tblnames, err := e.listTables(ctx, tx, sql.SchemaName{tn.Database, scname})
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

	return MakeVirtualTable(tn,
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

func (e *engine) makeColumnsTable(ctx context.Context, tx Transaction, tn sql.TableName) (Table,
	error) {

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	values := [][]sql.Value{}

	scnames, err := e.listSchemas(ctx, tx, tn.Database)
	if err != nil {
		return nil, err
	}

	for _, scname := range scnames {
		tblnames, err := e.listTables(ctx, tx, sql.SchemaName{tn.Database, scname})
		if err != nil {
			return nil, err
		}

		for _, tblname := range tblnames {
			ttn := sql.TableName{tn.Database, scname, tblname}
			if scname == sql.METADATA && tblname == sql.ID("columns") {
				values = appendColumns(values, ttn, columnsColumns, columnsColumnTypes)
			} else {
				tbl, err := e.LookupTable(ctx, tx, ttn)
				if err != nil {
					return nil, err
				}
				values = appendColumns(values, ttn, tbl.Columns(ctx), tbl.ColumnTypes(ctx))
			}
		}
	}

	return MakeVirtualTable(tn, columnsColumns, columnsColumnTypes, values), nil
}

func (e *engine) makeDatabasesTable(ctx context.Context, tx Transaction, tn sql.TableName) (Table,
	error) {

	dbnames, err := e.st.ListDatabases(ctx, tx)
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

	return MakeVirtualTable(tn, []sql.Identifier{sql.ID("database")},
		[]sql.ColumnType{sql.IdColType}, values), nil

}

func makeIdentifiersTable(ctx context.Context, tx Transaction, tn sql.TableName) (Table, error) {
	values := [][]sql.Value{}

	for id, n := range sql.Names {
		values = append(values,
			[]sql.Value{
				sql.StringValue(n),
				sql.Int64Value(id),
				sql.BoolValue(id.IsReserved()),
			})
	}

	return MakeVirtualTable(tn,
		[]sql.Identifier{sql.ID("name"), sql.ID("id"), sql.ID("reserved")},
		[]sql.ColumnType{sql.IdColType, sql.Int32ColType, sql.BoolColType}, values), nil
}

func makeConfigTable(ctx context.Context, tx Transaction, tn sql.TableName) (Table, error) {
	values := [][]sql.Value{}

	for _, v := range config.Vars() {
		values = append(values,
			[]sql.Value{
				sql.StringValue(v.Name()),
				sql.StringValue(v.By()),
				sql.StringValue(v.Val()),
			})
	}

	return MakeVirtualTable(tn,
		[]sql.Identifier{sql.ID("name"), sql.ID("by"), sql.ID("value")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.StringColType}, values), nil
}
