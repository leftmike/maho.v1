package stmt

import (
	"fmt"

	"maho/engine"
	"maho/expr"
	"maho/sql"
)

type FromItem interface {
	fmt.Stringer
}

type FromTableAlias TableAlias

/*
type FromSelect struct {
	Select Select
	Alias  sql.Identifier
}

type FromValues struct {
	Values Values
	Alias  sql.Identifier
}

type JoinType int

const (
	Join = iota // JOIN
	InnerJoin
	LeftJoin
	LeftOuterJoin
	RightJoin
	RightOuterJoin
	FullJoin
	FullOuterJoin
	CrossJoin
)

type FromJoin struct {
	Left    FromItem
	Right   FromItem
	Natural bool
	Type    JoinType
	On      sql.Expr
	Using   []sql.Identifier
}
*/

type SelectResult interface {
	fmt.Stringer
}

type TableResult struct {
	Table sql.Identifier
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
	Results []SelectResult
	From    FromItem
	Where   expr.Expr
}

func (fta FromTableAlias) String() string {
	return TableAlias(fta).String()
}

func (tr TableResult) String() string {
	return fmt.Sprintf("%s.*", tr.Table)
}

func (tcr TableColumnResult) String() string {
	var s string
	if tcr.Table == 0 {
		s = tcr.Column.String()
	} else {
		s = fmt.Sprintf("%s.%s", tcr.Table, tcr.Column)
	}
	if tcr.Alias != 0 {
		s += fmt.Sprintf(" AS %s", tcr.Alias)
	}
	return s
}

func (er ExprResult) String() string {
	s := er.Expr.String()
	if er.Alias != 0 {
		s += fmt.Sprintf(" AS %s", er.Alias)
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
			s += sr.String()
		}
	}
	s += fmt.Sprintf(" FROM %s", stmt.From)
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

func (stmt *Select) Execute(e *engine.Engine) (interface{}, error) {
	fmt.Println(stmt)

	fta, ok := stmt.From.(FromTableAlias)
	if !ok {
		return nil, fmt.Errorf("select: only table aliases supported now: %s", stmt.From)
	}
	db, err := e.LookupDatabase(fta.Database)
	if err != nil {
		return nil, err
	}
	tbl, err := db.Table(fta.Table)
	if err != nil {
		return nil, err
	}

	return tbl.Rows()
}

func SelectResultEqual(sr1, sr2 SelectResult) bool {
	switch sr1 := sr1.(type) {
	case TableResult:
		if sr2, ok := sr2.(TableResult); ok {
			return sr1 == sr2
		}
	case TableColumnResult:
		if sr2, ok := sr2.(TableColumnResult); ok {
			return sr1 == sr2
		}
	case ExprResult:
		if sr2, ok := sr2.(ExprResult); ok {
			return sr1.Alias == sr2.Alias && expr.DeepEqual(sr1.Expr, sr2.Expr)
		}
	default:
		panic(fmt.Sprintf("unexpected type for SelectResult: %T: %v", sr1, sr1))
	}
	return false
}
