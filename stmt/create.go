package stmt

import (
	"fmt"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type CreateTable struct {
	Table       TableName
	Columns     []sql.Identifier
	ColumnTypes []db.ColumnType
}

func (stmt *CreateTable) String() string {
	s := fmt.Sprintf("CREATE TABLE %s (", stmt.Table)

	for i, ct := range stmt.ColumnTypes {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s %s", stmt.Columns[i], ct.DataType())
		if ct.NotNull {
			s += " NOT NULL"
		}
		if ct.Default != nil {
			s += fmt.Sprintf(" DEFAULT %s", ct.Default)
		}
	}
	s += ")"
	return s
}

func (stmt *CreateTable) Plan(e *engine.Engine) (interface{}, error) {
	return stmt, nil
}

func (stmt *CreateTable) Execute(e *engine.Engine) (int64, error) {
	d, err := e.LookupDatabase(stmt.Table.Database)
	if err != nil {
		return 0, err
	}
	dbase, ok := d.(db.DatabaseModify)
	if !ok {
		return 0, fmt.Errorf("engine: database \"%s\" can't be modified", d.Name())
	}
	return 0, dbase.CreateTable(stmt.Table.Table, stmt.Columns, stmt.ColumnTypes)
}
