package query

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
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
	On    sql.Expr
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

type joinState int

const (
	matchRows joinState = iota
	rightRemaining
	allDone
)

type usingMatch struct {
	leftColIndex, rightColIndex int
}

type joinRows struct {
	state joinState

	leftRows engine.Rows
	haveLeft bool
	leftDest []sql.Value
	leftLen  int
	leftUsed bool
	needLeft bool

	rightRows  [][]sql.Value
	rightIndex int
	rightDest  []sql.Value
	rightLen   int
	rightUsed  []bool

	columns []sql.Identifier

	on expr.CExpr

	using    []usingMatch
	src2dest []int
}

func (jr *joinRows) Columns() []sql.Identifier {
	return jr.columns
}

func (jr *joinRows) Close() error {
	jr.state = allDone
	return jr.leftRows.Close()
}

func (jr *joinRows) EvalRef(idx int) sql.Value {
	if idx < jr.leftLen {
		return jr.leftDest[idx]
	}
	return jr.rightDest[idx-jr.leftLen]
}

func (jr *joinRows) onMatch(ctx context.Context, dest []sql.Value) (bool, error) {
	v, err := jr.on.Eval(ctx, jr)
	if err != nil {
		return true, err
	}
	b, ok := v.(sql.BoolValue)
	if !ok {
		return true, fmt.Errorf("engine: expected boolean result from ON condition: %s",
			sql.Format(v))
	}
	if b {
		jr.leftUsed = true
		if jr.rightUsed != nil {
			jr.rightUsed[jr.rightIndex-1] = true
		}
		copy(dest, jr.leftDest)
		copy(dest[jr.leftLen:], jr.rightDest)
		return true, nil
	}

	return false, nil
}

func (jr *joinRows) onUsing(dest []sql.Value) (bool, error) {
	for _, use := range jr.using {
		if jr.leftDest[use.leftColIndex] != jr.rightDest[use.rightColIndex] {
			return false, nil
		}
	}

	jr.leftUsed = true
	if jr.rightUsed != nil {
		jr.rightUsed[jr.rightIndex-1] = true
	}
	copy(dest, jr.leftDest)
	for destIndex, srcIndex := range jr.src2dest {
		dest[destIndex+jr.leftLen] = jr.rightDest[srcIndex]
	}
	return true, nil
}

func (jr *joinRows) Next(ctx context.Context, dest []sql.Value) error {
	if jr.state == allDone {
		return io.EOF
	} else if jr.state == rightRemaining {
		for jr.rightIndex < len(jr.rightRows) {
			if !jr.rightUsed[jr.rightIndex] {
				for idx := 0; idx < jr.leftLen; idx++ {
					dest[idx] = nil
				}
				if jr.using != nil {
					for destIndex, srcIndex := range jr.src2dest {
						dest[destIndex+jr.leftLen] = jr.rightRows[jr.rightIndex][srcIndex]
					}
				} else {
					copy(dest[jr.leftLen:], jr.rightRows[jr.rightIndex])
				}
				jr.rightIndex += 1
				return nil
			}

			jr.rightIndex += 1
		}

		jr.state = allDone
		return io.EOF
	}

	// jr.state == matchRows
	for {
		// Make sure that we have a left row.
		if !jr.haveLeft {
			err := jr.leftRows.Next(ctx, jr.leftDest)
			if err == io.EOF && jr.rightUsed != nil {
				jr.state = rightRemaining
				jr.rightIndex = 0
				return jr.Next(ctx, dest)
			}
			if err != nil {
				jr.state = allDone
				return err
			}
			jr.rightIndex = 0
			jr.haveLeft = true
			jr.leftUsed = false
		}

		if jr.rightIndex == len(jr.rightRows) {
			jr.haveLeft = false
		} else {
			// Get a right row.
			jr.rightDest = jr.rightRows[jr.rightIndex]
			jr.rightIndex += 1
			if jr.rightIndex == len(jr.rightRows) {
				jr.haveLeft = false
			}

			// Compare the left and right rows, and decide whether to combine and return them as a
			// result row.
			if jr.on != nil {
				if done, err := jr.onMatch(ctx, dest); done {
					return err
				}
			} else if jr.using != nil {
				if done, err := jr.onUsing(dest); done {
					return err
				}
			} else {
				copy(dest, jr.leftDest)
				copy(dest[jr.leftLen:], jr.rightDest)
				return nil
			}
		}

		// Check if the left row did not match any of the right rows and if we need it (LEFT JOIN
		// or FULL JOIN).
		if !jr.haveLeft && !jr.leftUsed && jr.needLeft {
			// Return the unused left row combined with a NULL right row as the result row.
			copy(dest, jr.leftDest)
			for idx := 0; idx < jr.rightLen; idx++ {
				dest[idx+jr.leftLen] = nil
			}
			return nil
		}
	}
}

func (_ *joinRows) Delete(ctx context.Context) error {
	return fmt.Errorf("join rows may not be deleted")
}

func (_ *joinRows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	return fmt.Errorf("join rows may not be updated")
}

func (fj FromJoin) rows(ses *evaluate.Session, tx engine.Transaction) (engine.Rows, *fromContext,
	error) {

	leftRows, leftCtx, err := fj.Left.rows(ses, tx)
	if err != nil {
		return nil, nil, err
	}
	rightRows, rightCtx, err := fj.Right.rows(ses, tx)
	if err != nil {
		return nil, nil, err
	}

	rrows, err := evaluate.AllRows(ses, rightRows)
	if err != nil {
		return nil, nil, err
	}
	leftLen := len(leftCtx.cols)
	rows := joinRows{
		leftRows:  leftRows,
		leftDest:  make([]sql.Value, leftLen),
		leftLen:   leftLen,
		rightRows: rrows,
	}
	if fj.Type == LeftJoin || fj.Type == FullJoin {
		rows.needLeft = true
	}
	if fj.Type == RightJoin || fj.Type == FullJoin {
		rows.rightUsed = make([]bool, len(rrows))
	}

	var fctx *fromContext
	if fj.Using != nil {
		useSet := map[sql.Identifier]struct{}{}
		for _, col := range fj.Using {
			var lcdx, rcdx int
			lcdx, err = leftCtx.usingIndex(col, "left")
			if err != nil {
				return nil, nil, err
			}
			rcdx, err = rightCtx.usingIndex(col, "right")
			if err != nil {
				return nil, nil, err
			}
			rows.using = append(rows.using, usingMatch{leftColIndex: lcdx, rightColIndex: rcdx})
			useSet[col] = struct{}{}
		}

		fctx, rows.src2dest = joinContextsUsing(leftCtx, rightCtx, useSet)
		rows.rightLen = len(rows.src2dest)
	} else {
		fctx = joinContextsOn(leftCtx, rightCtx)
		rows.rightLen = len(rightCtx.cols)
		if fj.On != nil {
			rows.on, err = expr.Compile(fctx, fj.On, false)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	rows.columns = fctx.columns()
	return &rows, fctx, nil
}
