package stmt

import (
	"fmt"
	"maho/sql"
)

type SelectResult struct {
	Table  sql.Identifier
	Column sql.Identifier
	Alias  sql.Identifier
}

type Select struct {
	Database sql.Identifier
	Table    sql.Identifier
	Alias    sql.Identifier
	Results  []SelectResult
}

func (stmt *Select) String() string {
	s := "SELECT "
	if stmt.Results == nil {
		s += "* "
	} else {
		for i, sr := range stmt.Results {
			if i > 0 {
				s += ", "
			}
			if sr.Table == 0 {
				s += sr.Column.String()
			} else {
				s += fmt.Sprintf("%s.%s", sr.Table, sr.Column)
			}
			if sr.Alias != sr.Column {
				s += fmt.Sprintf(" AS %s", sr.Alias)
			}
		}
	}
	if stmt.Database == 0 {
		s += fmt.Sprintf(" FROM %s", stmt.Table)
	} else {
		s += fmt.Sprintf(" FROM %s.%s", stmt.Database, stmt.Table)
	}
	if stmt.Alias != 0 {
		s += fmt.Sprintf(" AS %s", stmt.Alias)
	}
	return s
}

func (stmt *Select) Dispatch(e Executer) (interface{}, error) {
	return e.Select(stmt)
}
