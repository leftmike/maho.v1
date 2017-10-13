package stmt

import (
	"fmt"
	"reflect"

	"maho/engine"
	"maho/expr"
	"maho/sql"
)

type FromItem interface {
	fmt.Stringer
	equal(fi FromItem) bool
}

type FromTableAlias TableAlias

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
	NoJoin JoinType = iota
	Join
	InnerJoin
	LeftJoin
	LeftOuterJoin
	RightJoin
	RightOuterJoin
	FullJoin
	FullOuterJoin
	CrossJoin
)

var joinType = map[JoinType]string{
	Join:           "JOIN",
	InnerJoin:      "INNER JOIN",
	LeftJoin:       "LEFT JOIN",
	LeftOuterJoin:  "LEFT OUTER JOIN",
	RightJoin:      "RIGHT JOIN",
	RightOuterJoin: "RIGHT OUTER JOIN",
	FullJoin:       "FULL JOIN",
	FullOuterJoin:  "FULL OUTER JOIN",
	CrossJoin:      "CROSS JOIN",
}

type FromJoin struct {
	Left    FromItem
	Right   FromItem
	Natural bool
	Type    JoinType
	On      expr.Expr
	Using   []sql.Identifier
}

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

func (fta FromTableAlias) equal(fi FromItem) bool {
	if fta2, ok := fi.(FromTableAlias); ok && fta == fta2 {
		return true
	}
	return false
}

func (fs FromSelect) String() string {
	s := fmt.Sprintf("(%s)", fs.Select.String())
	if fs.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fs.Alias)
	}
	return s
}

func (_ FromSelect) equal(fi FromItem) bool {
	return false // XXX: investigate reflect.DeepEqual and why it is failing
}

func (fv FromValues) String() string {
	s := fmt.Sprintf("(%s)", fv.Values.String())
	if fv.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fv.Alias)
	}
	return s
}

func (_ FromValues) equal(fi FromItem) bool {
	return false // XXX: investigate reflect.DeepEqual and why it is failing
}

func (jt JoinType) String() string {
	return joinType[jt]
}

func (fj FromJoin) String() string {
	s := fj.Left.String()
	if fj.Natural {
		s += " NATURAL"
	}
	s += fmt.Sprintf(" %s ", fj.Type.String())
	s += fj.Right.String()
	if fj.On != nil {
		s += fmt.Sprintf(" ON %s", fj.On.String())
	}
	if len(fj.Using) > 0 {
		s += " USING ("
		for i, id := range fj.Using {
			if i > 0 {
				s += ", "
			}
			s += id.String()
		}
		s += ")"
	}
	return s
}

func (fj FromJoin) equal(fi FromItem) bool {
	fj2, ok := fi.(FromJoin)
	if !ok {
		return false
	}
	return FromItemEqual(fj.Left, fj2.Left) && FromItemEqual(fj.Right, fj2.Right) &&
		fj.Natural == fj2.Natural && fj.Type == fj2.Type && expr.DeepEqual(fj.On, fj2.On) &&
		reflect.DeepEqual(fj.Using, fj2.Using)
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

func FromItemEqual(fi1, fi2 FromItem) bool {
	if fi1 == nil {
		return fi2 == nil
	}
	return fi1.equal(fi2)
}
