package query

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

type ColumnUpdate struct {
	Column sql.Identifier
	Expr   expr.Expr
}

type Update struct {
	Table         sql.TableName
	ColumnUpdates []ColumnUpdate
	Where         expr.Expr
}

func (stmt *Update) String() string {
	s := fmt.Sprintf("UPDATE %s SET ", stmt.Table)
	for i, cu := range stmt.ColumnUpdates {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s = %s", cu.Column, cu.Expr)
	}
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

type columnUpdate struct {
	column int
	expr   sql.CExpr
}

type updatePlan struct {
	columns []sql.Identifier
	dest    []sql.Value
	rows    sql.Rows
	updates []columnUpdate
}

func (up *updatePlan) EvalRef(idx int) sql.Value {
	return up.dest[idx]
}

func (stmt *Update) Plan(ses *evaluate.Session, ctx context.Context, pe evaluate.PlanEngine,
	tx sql.Transaction) (evaluate.Plan, error) {

	tbl, tt, err := ses.Engine.LookupTable(ses.Context(), tx, ses.ResolveTableName(stmt.Table))
	if err != nil {
		return nil, err
	}
	rows, err := tbl.Rows(ses.Context(), nil, nil)
	if err != nil {
		return nil, err
	}

	fctx := makeFromContext(stmt.Table.Table, rows.Columns())
	if stmt.Where != nil {
		ce, err := expr.Compile(ses, tx, fctx, stmt.Where)
		if err != nil {
			return nil, err
		}
		rows = &filterRows{rows: rows, cond: ce}
	}

	plan := updatePlan{
		columns: tt.Columns(),
		rows:    rows,
		updates: make([]columnUpdate, 0, len(stmt.ColumnUpdates)),
	}

	for _, cu := range stmt.ColumnUpdates {
		ce, err := expr.Compile(ses, tx, fctx, cu.Expr)
		if err != nil {
			return nil, err
		}
		col, err := fctx.colIndex(cu.Column, "update")
		if err != nil {
			return nil, err
		}
		plan.updates = append(plan.updates, columnUpdate{column: col, expr: ce})
	}
	return &plan, nil
}

func (up *updatePlan) Explain() string {
	// XXX: updatePlan.Explain
	return ""
}

func (up *updatePlan) Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64,
	error) {

	up.dest = make([]sql.Value, len(up.rows.Columns()))
	cnt := int64(0)
	updates := make([]sql.ColumnUpdate, len(up.updates))

	for {
		err := up.rows.Next(ctx, up.dest)
		if err == io.EOF {
			return cnt, nil
		} else if err != nil {
			return -1, err
		}

		updates = updates[:0]
		for _, update := range up.updates {
			col := update.column
			val, err := update.expr.Eval(ctx, up)
			if err != nil {
				return -1, err
			}
			if sql.Compare(val, up.dest[col]) != 0 {
				updates = append(updates, sql.ColumnUpdate{Column: col, Value: val})
			}
		}

		if len(updates) > 0 {
			err = up.rows.Update(ctx, updates)
			if err != nil {
				up.rows.Close()
				return -1, err
			}
			cnt += 1
		}
	}
}
