package join

import (
	"fmt"

	"maho/db"
	"maho/engine"
	"maho/expr"
	"maho/sql"
)

type FromItem interface {
	fmt.Stringer
	Rows(e *engine.Engine) (db.Rows, error)
}

type FromTableAlias struct {
	Database sql.Identifier
	Table    sql.Identifier
	Alias    sql.Identifier
}

/*
- package query
- type Rows interface { db.Rows }
- query.Where(engine, rows, cond) (query.Rows, error)
- move join to query
- Rows(engine) (query.Rows, error) // instead of db.Rows
*/

type FromStmt struct {
	Stmt  FromItem
	Alias sql.Identifier
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

func (fta FromTableAlias) String() string {
	var s string
	if fta.Database == 0 {
		s = fta.Table.String()
	} else {
		s = fmt.Sprintf("%s.%s", fta.Database, fta.Table)
	}
	if fta.Table != fta.Alias {
		s += fmt.Sprintf(" AS %s", fta.Alias)
	}
	return s
}

func (fta FromTableAlias) Rows(e *engine.Engine) (db.Rows, error) {
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

func (fs FromStmt) String() string {
	s := fmt.Sprintf("(%s)", fs.Stmt.String())
	if fs.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fs.Alias)
	}
	return s
}

func (fs FromStmt) Rows(e *engine.Engine) (db.Rows, error) {
	return fs.Stmt.Rows(e)
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

func (fj FromJoin) Rows(e *engine.Engine) (db.Rows, error) {
	return nil, fmt.Errorf("FromJoin not implemented yet")
}
