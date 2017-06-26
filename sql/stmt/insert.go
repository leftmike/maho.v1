package stmt

import (
	"maho/sql"
)

type InsertValues struct {
	Table   TableName
	Columns []sql.Identifier
	Rows    [][]sql.Value
}

func (stmt *InsertValues) String() string {
	s := "INSERT INTO " + stmt.Table.String()
	if stmt.Columns != nil {
		s += "("
		for i, col := range stmt.Columns {
			if i > 0 {
				s += ", "
			}
			s += col.String()
		}
		s += ") "
	}

	s += "VALUES"

	for i, r := range stmt.Rows {
		if i > 0 {
			s += ", ("
		} else {
			s += " ("
		}

		for j, v := range r {
			if j > 0 {
				s += ", "
			}
			s += sql.FormatValue(v)
		}

		s += ")"
	}

	return s
}

func (stmt *InsertValues) Dispatch(e Executer) (interface{}, error) {
	return e.InsertValues(stmt)
}
