package stmt

import (
	"fmt"

	"maho/engine"
	"maho/expr"
	"maho/sql"
)

/*
type Subquery interface {
	subquery() bool
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

/*
OLD; delete
type SelectResult struct {
	Table  sql.Identifier
	Column sql.Identifier
	Alias  sql.Identifier
}
*/

type SelectResult interface {
	fmt.Stringer
}

type TableResult struct {
	Table sql.Identifier
}

type ColumnResult struct {
	Column sql.Identifier
	Alias  sql.Identifier
}

type TableColumnResult struct {
	Table  sql.Identifier
	Column sql.Identifier
	Alias  sql.Identifier
}

type ExprResult struct {
	Expr  expr.Expr
	Alias sql.Identifier
}

type Select struct {
	Table   TableAlias
	Results []SelectResult
	Where   expr.Expr
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
			s += sr.String()
		}
	}
	s += fmt.Sprintf(" FROM %s", stmt.Table)
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

func (tcr TableColumnResult) String() string {
	var s string
	if tcr.Table == 0 {
		s = tcr.Column.String()
	} else {
		s = fmt.Sprintf("%s.%s", tcr.Table, tcr.Column)
	}
	if tcr.Table != tcr.Alias {
		s += fmt.Sprintf(" AS %s", tcr.Alias)
	}
	return s
}

func (stmt *Select) Execute(e *engine.Engine) (interface{}, error) {
	fmt.Println(stmt)

	db, err := e.LookupDatabase(stmt.Table.Database)
	if err != nil {
		return nil, err
	}
	tbl, err := db.Table(stmt.Table.Table)
	if err != nil {
		return nil, err
	}

	return tbl.Rows()
}
