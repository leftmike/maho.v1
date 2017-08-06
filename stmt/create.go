package stmt

import (
	"fmt"

	"maho/engine"
	"maho/sql"
)

type CreateTable struct {
	Table   TableName
	Columns []sql.Column
}

func (stmt *CreateTable) String() string {
	s := fmt.Sprintf("CREATE TABLE %s (", stmt.Table)

	for i, col := range stmt.Columns {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s %s", col.Name, col.DataType())
		if col.Type == sql.IntegerType && col.Width < 255 {
			s += fmt.Sprintf("(%d)", col.Width)
		} else if col.Type == sql.DoubleType && (col.Width < 255 || col.Fraction < 30) {
			s += fmt.Sprintf("(%d, %d)", col.Width, col.Fraction)
		}
		if col.NotNull {
			s += " NOT NULL"
		}
		if col.Default != nil {
			s += fmt.Sprintf(" DEFAULT %s", sql.Format(col.Default))
		}
	}
	s += ")"
	return s
}

func (stmt *CreateTable) Execute(e *engine.Engine) (interface{}, error) {
	fmt.Println(stmt)

	db, err := e.LookupDatabase(stmt.Table.Database)
	if err != nil {
		return nil, err
	}
	return nil, db.CreateTable(stmt.Table.Table, stmt.Columns)
}
