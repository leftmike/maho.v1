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
		if ct.Type == sql.IntegerType && ct.Width < 255 {
			s += fmt.Sprintf("(%d)", ct.Width)
		} else if ct.Type == sql.DoubleType && (ct.Width < 255 || ct.Fraction < 30) {
			s += fmt.Sprintf("(%d, %d)", ct.Width, ct.Fraction)
		}
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
	fmt.Println(stmt)

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
