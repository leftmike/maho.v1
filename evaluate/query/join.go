package query

import (
	"context"
	"fmt"
	"io"
	"strings"

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

func (fj FromJoin) resolve(ses *evaluate.Session) {
	fj.Left.resolve(ses)
	fj.Right.resolve(ses)
	if fj.On != nil {
		fj.On.Resolve(ses)
	}
}

type joinOp struct {
	typ JoinType

	leftRowsOp rowsOp
	leftLen    int
	needLeft   bool

	rightRowsOp   rowsOp
	rightLen      int
	needRightUsed bool

	columns []sql.Identifier

	on sql.CExpr

	using    []usingMatch
	src2dest []int
}

func (jo joinOp) explain() string {
	s := strings.ToLower(jo.typ.String())
	for _, col := range jo.columns {
		s += " " + col.String()
	}
	if jo.on != nil {
		s += " on " + jo.on.String()
	}
	return s
}

func (jo joinOp) children() []rowsOp {
	return []rowsOp{jo.leftRowsOp, jo.rightRowsOp}
}

func (jo joinOp) rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	leftRows, err := jo.leftRowsOp.rows(ctx, tx)
	if err != nil {
		return nil, err
	}

	rows, err := jo.rightRowsOp.rows(ctx, tx)
	if err != nil {
		return nil, err
	}

	rightRows, err := evaluate.AllRows(ctx, rows)
	if err != nil {
		return nil, err
	}
	var rightUsed []bool
	if jo.needRightUsed {
		rightUsed = make([]bool, len(rightRows))
	}

	return &joinRows{
		tx:        tx,
		leftRows:  leftRows,
		leftDest:  make([]sql.Value, jo.leftLen),
		leftLen:   jo.leftLen,
		rightRows: rightRows,
		rightUsed: rightUsed,
		needLeft:  jo.needLeft,
		using:     jo.using,
		src2dest:  jo.src2dest,
		rightLen:  jo.rightLen,
		on:        jo.on,
		columns:   jo.columns,
	}, nil
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
	tx sql.Transaction

	state joinState

	leftRows sql.Rows
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

	on sql.CExpr

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
	v, err := jr.on.Eval(ctx, jr.tx, jr)
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

func (fj FromJoin) plan(ctx context.Context, tx sql.Transaction) (rowsOp, *fromContext, error) {
	leftRowsOp, leftCtx, err := fj.Left.plan(ctx, tx)
	if err != nil {
		return nil, nil, err
	}
	rightRowsOp, rightCtx, err := fj.Right.plan(ctx, tx)
	if err != nil {
		return nil, nil, err
	}

	jop := joinOp{
		typ:         fj.Type,
		leftRowsOp:  leftRowsOp,
		leftLen:     len(leftCtx.cols),
		rightRowsOp: rightRowsOp,
	}
	if fj.Type == LeftJoin || fj.Type == FullJoin {
		jop.needLeft = true
	}
	if fj.Type == RightJoin || fj.Type == FullJoin {
		jop.needRightUsed = true
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
			jop.using = append(jop.using, usingMatch{leftColIndex: lcdx, rightColIndex: rcdx})
			useSet[col] = struct{}{}
		}

		fctx, jop.src2dest = joinContextsUsing(leftCtx, rightCtx, useSet)
		jop.rightLen = len(jop.src2dest)
	} else {
		fctx = joinContextsOn(leftCtx, rightCtx)
		jop.rightLen = len(rightCtx.cols)
		if fj.On != nil {
			jop.on, err = expr.Compile(ctx, tx, fctx, fj.On)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	jop.columns = fctx.columns()
	return jop, fctx, nil
}
