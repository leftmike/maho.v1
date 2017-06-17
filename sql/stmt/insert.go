package stmt

import (
	"fmt"
	"maho/sql"
)

type InsertInto struct {
	Database sql.Identifier
	Table    sql.Identifier
}

type InsertValues struct {
	InsertInto
	Columns []sql.Identifier
	Rows    [][]sql.Value
}

func (stmt *InsertInto) String() string {
	s := "INSERT INTO "
	if stmt.Database == 0 {
		s += fmt.Sprintf("%s ", stmt.Table)
	} else {
		s += fmt.Sprintf("%s.%s ", stmt.Database, stmt.Table)
	}
	return s
}

func (stmt *InsertValues) String() string {
	s := stmt.InsertInto.String()
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
