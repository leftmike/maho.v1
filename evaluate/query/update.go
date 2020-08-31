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
	tn      sql.TableName
	where   sql.CExpr
	dest    []sql.Value
	updates []columnUpdate
}

func (up *updatePlan) EvalRef(idx int) sql.Value {
	return up.dest[idx]
}

func (stmt *Update) Resolve(ses *evaluate.Session) {
	stmt.Table = ses.ResolveTableName(stmt.Table)
}

func (stmt *Update) Plan(ctx context.Context, pctx evaluate.PlanContext) (evaluate.Plan, error) {
	tt, err := pctx.Transaction().LookupTableType(ctx, stmt.Table)
	if err != nil {
		return nil, err
	}

	fctx := makeFromContext(stmt.Table.Table, tt.Columns())
	var where sql.CExpr
	if stmt.Where != nil {
		where, err = expr.Compile(ctx, pctx, fctx, stmt.Where)
		if err != nil {
			return nil, err
		}
	}

	plan := updatePlan{
		tn:      stmt.Table,
		where:   where,
		dest:    make([]sql.Value, len(tt.Columns())),
		updates: make([]columnUpdate, 0, len(stmt.ColumnUpdates)),
	}

	for _, cu := range stmt.ColumnUpdates {
		ce, err := expr.Compile(ctx, pctx, fctx, cu.Expr)
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

func (up *updatePlan) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	tbl, _, err := tx.LookupTable(ctx, up.tn)
	if err != nil {
		return -1, err
	}
	rows, err := tbl.Rows(ctx, nil, nil)
	if err != nil {
		return -1, err
	}
	if up.where != nil {
		rows = &filterRows{rows: rows, cond: up.where}
	}
	defer rows.Close()

	updates := make([]sql.ColumnUpdate, len(up.updates))
	var cnt int64
	for {
		err := rows.Next(ctx, up.dest)
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
			err = rows.Update(ctx, updates)
			if err != nil {
				return -1, err
			}
			cnt += 1
		}
	}
}
