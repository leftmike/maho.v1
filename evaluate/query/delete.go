package query

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

type Delete struct {
	Table sql.TableName
	Where expr.Expr
}

func (stmt *Delete) String() string {
	s := fmt.Sprintf("DELETE FROM %s", stmt.Table)
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

type deletePlan struct {
	rows sql.Rows
}

func (stmt *Delete) Plan(ctx context.Context, ses *evaluate.Session, pe evaluate.PlanEngine,
	tx sql.Transaction) (evaluate.Plan, error) {

	rows, err := lookupRows(ses, tx, stmt.Table)
	if err != nil {
		return nil, err
	}
	if stmt.Where != nil {
		ce, err := expr.Compile(ses, tx, makeFromContext(stmt.Table.Table, rows.Columns()),
			stmt.Where)
		if err != nil {
			return nil, err
		}
		rows = &filterRows{rows: rows, cond: ce}
	}
	return &deletePlan{rows: rows}, nil
}

func (dp *deletePlan) Explain() string {
	// XXX: deletePlan.Explain
	return ""
}

func (dp *deletePlan) Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64,
	error) {

	dest := make([]sql.Value, len(dp.rows.Columns()))
	cnt := int64(0)
	for {
		err := dp.rows.Next(ctx, dest)
		if err == io.EOF {
			return cnt, nil
		} else if err != nil {
			return cnt, err
		}
		err = dp.rows.Delete(ctx)
		if err != nil {
			return cnt, err
		}
		cnt += 1
	}
}
