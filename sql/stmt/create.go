package stmt

import (
	"fmt"
	"maho/sql"
)

type CreateTable struct {
	Database sql.Identifier
	Table    sql.Identifier
	Columns  []sql.Column
}

func (stmt *CreateTable) String() string {
	s := "CREATE TABLE "
	if stmt.Database == 0 {
		s += fmt.Sprintf("%s (", stmt.Table)
	} else {
		s += fmt.Sprintf("%s.%s (", stmt.Database, stmt.Table)
	}

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
			s += fmt.Sprintf(" DEFAULT %s", sql.FormatValue(col.Default))
		}
	}
	s += ")"
	return s
}

func (stmt *CreateTable) Dispatch(e Executer) (interface{}, error) {
	return e.CreateTable(stmt)
}
