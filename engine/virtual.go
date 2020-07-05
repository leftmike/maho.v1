package engine

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/sql"
)

func MakeVirtualTable(tn sql.TableName, cols []sql.Identifier, colTypes []sql.ColumnType,
	values [][]sql.Value) (sql.Table, sql.TableType, error) {

	tt := MakeTableType(cols, colTypes, nil)
	return &virtualTable{
		tn:     tn,
		tt:     tt,
		values: values,
	}, tt, nil
}

type virtualTable struct {
	tn     sql.TableName
	tt     sql.TableType
	values [][]sql.Value
}

type virtualRows struct {
	tn    sql.TableName
	cols  []sql.Identifier
	rows  [][]sql.Value
	index int
}

func (vt *virtualTable) Columns(ctx context.Context) []sql.Identifier {
	return vt.tt.Columns()
}

func (vt *virtualTable) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return vt.tt.ColumnTypes()
}

func (vt *virtualTable) PrimaryKey(ctx context.Context) []sql.ColumnKey {
	return vt.tt.PrimaryKey()
}

func (vt *virtualTable) Rows(ctx context.Context, minRow, maxRow []sql.Value) (sql.Rows, error) {
	if minRow != nil || maxRow != nil {
		panic("virtual: not implemented: minRow != nil || maxRow != nil")
	}
	return &virtualRows{tn: vt.tn, cols: vt.tt.Columns(), rows: vt.values}, nil
}

func (vt *virtualTable) Insert(ctx context.Context, row []sql.Value) error {
	return fmt.Errorf("virtual: table %s can not be modified", vt.tn)
}

func (vr *virtualRows) Columns() []sql.Identifier {
	return vr.cols
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

func (e *Engine) listSchemas(ctx context.Context, tx sql.Transaction,
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

func (e *Engine) makeSchemasTable(ctx context.Context, tx sql.Transaction,
	tn sql.TableName) (sql.Table, sql.TableType, error) {

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	values := [][]sql.Value{}

	scnames, err := e.listSchemas(ctx, tx, tn.Database)
	if err != nil {
		return nil, nil, err
	}

	for _, scname := range scnames {
		values = append(values, []sql.Value{
			sql.StringValue(tn.Database.String()),
			sql.StringValue(scname.String()),
		})
	}

	return MakeVirtualTable(tn,
		[]sql.Identifier{sql.ID("database_name"), sql.ID("schema_name")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType}, values)
}

func (e *Engine) listTables(ctx context.Context, tx sql.Transaction,
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

func (e *Engine) makeTablesTable(ctx context.Context, tx sql.Transaction,
	tn sql.TableName) (sql.Table, sql.TableType, error) {

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	values := [][]sql.Value{}

	scnames, err := e.listSchemas(ctx, tx, tn.Database)
	if err != nil {
		return nil, nil, err
	}

	for _, scname := range scnames {
		tblnames, err := e.listTables(ctx, tx, sql.SchemaName{tn.Database, scname})
		if err != nil {
			return nil, nil, err
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
		[]sql.ColumnType{sql.IdColType, sql.IdColType}, values)
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
		if ct.DefaultExpr != "" {
			def = sql.StringValue(ct.DefaultExpr)
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

func (e *Engine) makeColumnsTable(ctx context.Context, tx sql.Transaction,
	tn sql.TableName) (sql.Table, sql.TableType, error) {

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	values := [][]sql.Value{}

	scnames, err := e.listSchemas(ctx, tx, tn.Database)
	if err != nil {
		return nil, nil, err
	}

	for _, scname := range scnames {
		tblnames, err := e.listTables(ctx, tx, sql.SchemaName{tn.Database, scname})
		if err != nil {
			return nil, nil, err
		}

		for _, tblname := range tblnames {
			ttn := sql.TableName{tn.Database, scname, tblname}
			if scname == sql.METADATA && tblname == sql.ID("columns") {
				values = appendColumns(values, ttn, columnsColumns, columnsColumnTypes)
			} else {
				_, tt, err := e.LookupTable(ctx, tx, ttn)
				if err != nil {
					return nil, nil, err
				}
				values = appendColumns(values, ttn, tt.Columns(), tt.ColumnTypes())
			}
		}
	}

	return MakeVirtualTable(tn, columnsColumns, columnsColumnTypes, values)
}

func (e *Engine) makeDatabasesTable(ctx context.Context, tx sql.Transaction,
	tn sql.TableName) (sql.Table, sql.TableType, error) {

	dbnames, err := e.st.ListDatabases(ctx, tx)
	if err != nil {
		return nil, nil, err
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
		[]sql.ColumnType{sql.IdColType}, values)

}

func makeIdentifiersTable(ctx context.Context, tx sql.Transaction, tn sql.TableName) (sql.Table,
	sql.TableType, error) {

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
		[]sql.ColumnType{sql.IdColType, sql.Int32ColType, sql.BoolColType}, values)
}

func makeConfigTable(ctx context.Context, tx sql.Transaction, tn sql.TableName) (sql.Table,
	sql.TableType, error) {

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
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.StringColType}, values)
}
