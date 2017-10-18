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

func (fj FromJoin) rows(e *engine.Engine) (db.Rows, fromContext, error) {
	return nil, nil, fmt.Errorf("FromJoin.rows not implemented yet")
}
