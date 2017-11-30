package query

import (
	"fmt"
	"io"

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

// AllRows returns all of the rows from a db.Rows as slices of values.
func AllRows(rows db.Rows) ([][]sql.Value, error) {
	all := [][]sql.Value{}
	l := len(rows.Columns())
	for {
		dest := make([]sql.Value, l)
		err := rows.Next(dest)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		all = append(all, dest)
	}
	return all, nil
}

type joinRows struct {
	leftRows     db.Rows
	haveLeftDest bool
	leftDest     []sql.Value
	leftUsed     bool

	rightRows  [][]sql.Value
	rightIndex int
	rightUsed  []bool

	columns []sql.Identifier
}

func (jr *joinRows) Columns() []sql.Identifier {
	return jr.columns
}

func (jr *joinRows) Close() error {
	jr.haveLeftDest = false
	return jr.leftRows.Close()
}

func (jr *joinRows) next() error {
	if !jr.haveLeftDest || jr.rightIndex == len(jr.rightRows) {
		if len(jr.rightRows) == 0 {
			return io.EOF
		}
		err := jr.leftRows.Next(jr.leftDest)
		if err != nil {
			return err
		}
		jr.haveLeftDest = true
		jr.rightIndex = 0
		jr.leftUsed = false
	}
	return nil
}

type joinOnRows struct {
	joinRows
	on   expr.CExpr
	dest []sql.Value
}

func (jor *joinOnRows) EvalRef(idx int) sql.Value {
	return jor.dest[idx]
}

func (jor *joinOnRows) Next(dest []sql.Value) error {
	for {
		err := jor.next()
		if err != nil {
			return err
		}

		copy(dest, jor.leftDest)
		leftLen := len(jor.leftDest)
		rightLen := len(jor.rightRows[jor.rightIndex])
		for idx := 0; idx < rightLen; idx++ {
			dest[idx+leftLen] = jor.rightRows[jor.rightIndex][idx]
		}
		jor.rightIndex += 1

		if jor.on == nil {
			break
		}
		jor.dest = dest
		defer func() {
			jor.dest = nil
		}()
		v, err := jor.on.Eval(jor)
		if err != nil {
			return err
		}
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("expected boolean result from ON condition: %s", sql.Format(v))
		}
		if b {
			jor.leftUsed = true
			if jor.rightUsed != nil {
				jor.rightUsed[jor.rightIndex-1] = true
			}
			break
		}
	}
	return nil
}

type joinUsingRows struct {
	joinRows
}

func (fj FromJoin) rows(e *engine.Engine) (db.Rows, *fromContext, error) {
	leftRows, leftCtx, err := fj.Left.rows(e)
	if err != nil {
		return nil, nil, err
	}
	rightRows, rightCtx, err := fj.Right.rows(e)
	if err != nil {
		return nil, nil, err
	}

	fctx := joinContexts(leftCtx, rightCtx)
	rrows, err := AllRows(rightRows)
	if err != nil {
		return nil, nil, err
	}
	rows := joinOnRows{
		joinRows: joinRows{
			leftRows:  leftRows,
			rightRows: rrows,
			leftDest:  make([]sql.Value, len(leftCtx.cols)),
			columns:   fctx.columns(),
		},
	}
	if fj.On != nil {
		rows.on, err = expr.Compile(fctx, fj.On)
		if err != nil {
			return nil, nil, err
		}
	}
	// fj.JoinType == CrossJoin || fj.JoinType == Join
	// fj.Using == nil
	return &rows, fctx, nil
}
