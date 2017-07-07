package stmt

import (
	"fmt"
	"maho/sql"
	"maho/sql/expr"
)

type SelectResult struct {
	Table  sql.Identifier
	Column sql.Identifier
	Alias  sql.Identifier
}

type AliasTableName struct {
	TableName
	Alias sql.Identifier
}

type Select struct {
	Table   AliasTableName
	Results []SelectResult
	Where   expr.Expr
}

func (atn AliasTableName) String() string {
	s := atn.TableName.String()
	if atn.Table != atn.Alias {
		s += fmt.Sprintf(" AS %s", atn.Alias)
	}
	return s
}

func (stmt *Select) String() string {
	s := "SELECT "
	if stmt.Results == nil {
		s += "*"
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
	s += fmt.Sprintf(" FROM %s", stmt.Table)
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

func (stmt *Select) Dispatch(e Executer) (interface{}, error) {
	return e.Select(stmt)
}
