package query

import (
	"fmt"

	"maho/db"
	"maho/engine"
	"maho/expr"
	"maho/sql"
)

type JoinType int

const (
	NoJoin JoinType = iota

	Join      // INNER JOIN
	LeftJoin  // LEFT OUTER JOIN
	RightJoin // RIGHT OUTER JOIN
	FullJoin  // FULL OUTER JOIN
	CrossJoin
)

var joinType = map[JoinType]string{
	Join:      "JOIN",
	LeftJoin:  "LEFT JOIN",
	RightJoin: "RIGHT JOIN",
	FullJoin:  "FULL JOIN",
	CrossJoin: "CROSS JOIN",
}

type FromJoin struct {
	Left  FromItem
	Right FromItem
	Type  JoinType
	On    expr.Expr
	Using []sql.Identifier
}

func (jt JoinType) String() string {
	return joinType[jt]
}

func (fj FromJoin) String() string {
	s := fmt.Sprintf("%s %s %s", fj.Left, fj.Type.String(), fj.Right)
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

func (fj FromJoin) rows(e *engine.Engine) (db.Rows, *fromContext, error) {
	return nil, nil, fmt.Errorf("FromJoin.rows not implemented yet")
}
