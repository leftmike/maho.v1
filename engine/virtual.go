package engine

import (
	"context"
	"fmt"
	"io"

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
	tn      sql.TableName
	numCols int
	rows    [][]sql.Value
	index   int
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

func (vt *virtualTable) ModifyStart(ctx context.Context, event int64) error {
	return fmt.Errorf("virtual: table %s can not be modified", vt.tn)
}

func (vt *virtualTable) ModifyDone(ctx context.Context, event, cnt int64) (int64, error) {
	panic(fmt.Sprintf("virtual: table %s can not be modified", vt.tn))
}

func (vt *virtualTable) Rows(ctx context.Context, minRow, maxRow []sql.Value) (sql.Rows, error) {
	if minRow != nil || maxRow != nil {
		panic("virtual: not implemented: minRow != nil || maxRow != nil")
	}
	return &virtualRows{tn: vt.tn, numCols: len(vt.tt.Columns()), rows: vt.values}, nil
}

func (vt *virtualTable) IndexRows(ctx context.Context, iidx int,
	minRow, maxRow []sql.Value) (sql.IndexRows, error) {

	panic(fmt.Sprintf("virtual tables don't have indexes: %s", vt.tn))
}

func (vt *virtualTable) Insert(ctx context.Context, row []sql.Value) error {
	panic(fmt.Sprintf("virtual: table %s can not be modified", vt.tn))
}

func (vr *virtualRows) NumColumns() int {
	return vr.numCols
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
	panic(fmt.Sprintf("virtual: table %s can not be modified", vr.tn))
}

func (vr *virtualRows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	panic(fmt.Sprintf("virtual: table %s can not be modified", vr.tn))
}

func (e *Engine) listSchemas(ctx context.Context, tx sql.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	if dbname == sql.SYSTEM {
		dbnames, err := e.st.ListDatabases(ctx, tx.(*transaction).tx)
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

	scnames, err := e.st.ListSchemas(ctx, tx.(*transaction).tx, dbname)
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
	return e.st.ListTables(ctx, tx.(*transaction).tx, sn)
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
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType}, values)
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
	constraintsColumns = []sql.Identifier{sql.ID("database_name"), sql.ID("schema_name"),
		sql.ID("table_name"), sql.ID("constraint_name"), sql.ID("constraint_type"),
		sql.ID("details"),
	}
	constraintsColumnTypes = []sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType,
		sql.ColumnType{Type: sql.StringType, Size: sql.MaxIdentifier},
		sql.IdColType, sql.NullStringColType,
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
			if scname == sql.METADATA && tblname == sql.COLUMNS {
				values = appendColumns(values, ttn, columnsColumns, columnsColumnTypes)
			} else if scname == sql.METADATA && tblname == sql.CONSTRAINTS {
				values = appendColumns(values, ttn, constraintsColumns, constraintsColumnTypes)
			} else {
				tt, err := tx.LookupTableType(ctx, ttn)
				if err != nil {
					return nil, nil, err
				}
				values = appendColumns(values, ttn, tt.Columns(), tt.ColumnTypes())
			}
		}
	}

	return MakeVirtualTable(tn, columnsColumns, columnsColumnTypes, values)
}

func columnConstraintName(colNum int, tt *TableType, ct sql.ConstraintType) sql.Value {
	for _, con := range tt.constraints {
		if con.colNum == colNum && con.typ == ct {
			return sql.StringValue(con.name.String())
		}
	}

	return nil
}

func columnKey(tt *TableType, key []sql.ColumnKey) string {
	s := "("
	for i, ck := range key {
		if i > 0 {
			s += ", "
		}

		col := ck.Column()
		if col < len(tt.cols) {
			s += tt.cols[col].String()
		} else {
			s += fmt.Sprintf("<column %d>", col)
		}

		if ck.Reverse() {
			s += " DESC"
		} else {
			s += " ASC"
		}
	}

	return s + ")"
}

func appendConstraints(values [][]sql.Value, tn sql.TableName, tt *TableType) [][]sql.Value {
	for i, ct := range tt.colTypes {
		if ct.DefaultExpr != "" {
			values = append(values,
				[]sql.Value{
					sql.StringValue(tn.Database.String()),
					sql.StringValue(tn.Schema.String()),
					sql.StringValue(tn.Table.String()),
					columnConstraintName(i, tt, sql.DefaultConstraint),
					sql.StringValue("DEFAULT"),
					sql.StringValue("column " + tt.cols[i].String() + ": " + ct.DefaultExpr),
				})
		}
		if ct.NotNull {
			values = append(values,
				[]sql.Value{
					sql.StringValue(tn.Database.String()),
					sql.StringValue(tn.Schema.String()),
					sql.StringValue(tn.Table.String()),
					columnConstraintName(i, tt, sql.NotNullConstraint),
					sql.StringValue("NOT NULL"),
					sql.StringValue("column " + tt.cols[i].String()),
				})
		}
	}

	for _, chk := range tt.checks {
		values = append(values,
			[]sql.Value{
				sql.StringValue(tn.Database.String()),
				sql.StringValue(tn.Schema.String()),
				sql.StringValue(tn.Table.String()),
				sql.StringValue(chk.name.String()),
				sql.StringValue("CHECK"),
				sql.StringValue(chk.checkExpr),
			})
	}

	for _, it := range tt.indexes {
		if it.Unique {
			values = append(values,
				[]sql.Value{
					sql.StringValue(tn.Database.String()),
					sql.StringValue(tn.Schema.String()),
					sql.StringValue(tn.Table.String()),
					sql.StringValue(it.Name.String()),
					sql.StringValue("UNIQUE"),
					sql.StringValue(columnKey(tt, it.Key)),
				})
		}
	}

	if len(tt.primary) > 0 {
		values = append(values,
			[]sql.Value{
				sql.StringValue(tn.Database.String()),
				sql.StringValue(tn.Schema.String()),
				sql.StringValue(tn.Table.String()),
				nil,
				sql.StringValue("PRIMARY"),
				sql.StringValue(columnKey(tt, tt.primary)),
			})
	}

	return values
}

func (e *Engine) makeConstraintsTable(ctx context.Context, tx sql.Transaction,
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
			if scname == sql.METADATA && tblname == sql.COLUMNS {
				values = appendConstraints(values, ttn,
					MakeTableType(columnsColumns, columnsColumnTypes, nil))
			} else if scname == sql.METADATA && tblname == sql.CONSTRAINTS {
				values = appendConstraints(values, ttn,
					MakeTableType(constraintsColumns, constraintsColumnTypes, nil))
			} else {
				tt, err := tx.LookupTableType(ctx, ttn)
				if err != nil {
					return nil, nil, err
				}
				if tt, ok := tt.(*TableType); ok {
					values = appendConstraints(values, ttn, tt)
				}
			}
		}
	}

	return MakeVirtualTable(tn, constraintsColumns, constraintsColumnTypes, values)
}

func (e *Engine) makeDatabasesTable(ctx context.Context, tx sql.Transaction,
	tn sql.TableName) (sql.Table, sql.TableType, error) {

	dbnames, err := e.st.ListDatabases(ctx, tx.(*transaction).tx)
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
