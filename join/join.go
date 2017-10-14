package join

import (
	"fmt"

	"maho/db"
	"maho/engine"
	"maho/expr"
	"maho/sql"
	"maho/stmt"
)

type FromTableAlias stmt.TableAlias

type FromSelect struct {
	Select *stmt.Select
	Alias  sql.Identifier
}

type FromValues struct {
	Values *stmt.Values
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
	Left    stmt.FromItem
	Right   stmt.FromItem
	Natural bool
	Type    JoinType
	On      expr.Expr
	Using   []sql.Identifier
}

func (fta FromTableAlias) String() string {
	return stmt.TableAlias(fta).String()
}

func (fta FromTableAlias) Join(e *engine.Engine) (db.Rows, error) {
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

func (fs FromSelect) String() string {
	s := fmt.Sprintf("(%s)", fs.Select.String())
	if fs.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fs.Alias)
	}
	return s
}

func (fs FromSelect) Join(e *engine.Engine) (db.Rows, error) {
	return nil, fmt.Errorf("FromSelect not implemented yet")
}

func (fv FromValues) String() string {
	s := fmt.Sprintf("(%s)", fv.Values.String())
	if fv.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fv.Alias)
	}
	return s
}

func (fv FromValues) Join(e *engine.Engine) (db.Rows, error) {
	return nil, fmt.Errorf("FromValues not implemented yet")
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

func (fj FromJoin) Join(e *engine.Engine) (db.Rows, error) {
	return nil, fmt.Errorf("FromJoin not implemented yet")
}
