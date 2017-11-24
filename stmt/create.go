package stmt

import (
	"fmt"

	"maho/db"
	"maho/engine"
	"maho/sql"
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
			s += fmt.Sprintf(" DEFAULT %s", sql.Format(ct.Default))
		}
	}
	s += ")"
	return s
}

func (stmt *CreateTable) Execute(e *engine.Engine) (interface{}, error) {
	d, err := e.LookupDatabase(stmt.Table.Database)
	if err != nil {
		return nil, err
	}
	dbase, ok := d.(db.DatabaseModify)
	if !ok {
		return nil, fmt.Errorf("\"%s\" database can't be modified", d.Name())
	}
	return nil, dbase.CreateTable(stmt.Table.Table, stmt.Columns, stmt.ColumnTypes)
}
