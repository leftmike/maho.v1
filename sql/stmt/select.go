package stmt

import (
	"fmt"
	"maho/sql"
)

/*
type ResultColumn struct {
	ID   sql.Identifier // Table or ColumnAlias
	Expr sql.Expr
}

func (rc ResultColumn) Table() sql.Identifier {
	return rc.ID
}

func (rc ResultColumn) ColumnAlias() sql.Identifier {
	return rc.ID
}

func (rc ResultColumn) IsExpr() bool {
	return rc.Expr != nil
}

func (rc ResultColumn) IsTable() bool {
	return rc.Expr == nil
}

type Subquery interface {
	subquery() bool
}

type TableAlias struct {
	TableName
	Alias sql.Identifier
}

func (ta *TableAlias) subquery() bool {
	return true
}

type FromItem struct {
	Alias    sql.Identifier
	Subquery interface{} // Select, Values, TableName, or Join
}

type JoinType int

const (
	PlainJoin = iota // JOIN
	InnerJoin
	CrossJoin
	LeftJoin
)

type Join struct {
	Left    interface{}
	Right   interface{}
	Natural bool
	Outer   bool
	Type    JoinType
	On      sql.Expr
	Using   []sql.Identifier
}

type NewSelect struct {
	Results []ResultColumn
	From    FromItem
	Where   sql.Expr
}

func (s *NewSelect) subquery() bool {
	return true
}

func (s *NewSelect) AllResults() bool { // SELECT * ...
	return s.Results == nil
}
*/

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
	Where   sql.Expr
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
