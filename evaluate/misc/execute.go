package misc

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

type Execute struct {
	Name   sql.Identifier
	Params []expr.Expr
}

func (stmt *Execute) String() string {
	return fmt.Sprintf("EXECUTE %s (", stmt.Name)
}

func (stmt *Execute) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction) (evaluate.Plan, error) {

	prep := pctx.GetPreparedPlan(stmt.Name)
	if prep == nil {
		return nil, fmt.Errorf("engine: prepared statement not found: %s", stmt.Name)
	}

	params := make([]sql.CExpr, 0, len(stmt.Params))
	for _, param := range stmt.Params {
		ce, err := expr.Compile(ctx, pctx, tx, nil, param)
		if err != nil {
			return nil, err
		}
		params = append(params, ce)
	}

	if prepStmt, ok := prep.(*evaluate.PreparedStmtPlan); ok {
		return executeStmtPlan{
			params:   params,
			prepStmt: prepStmt,
		}, nil
	} else if prepRows, ok := prep.(*evaluate.PreparedRowsPlan); ok {
		return executeRowsPlan{
			params:   params,
			prepRows: prepRows,
		}, nil
	}
	panic(fmt.Sprintf("expected stmt or rows plan; got %#v", prep))
}

func setParameters(ctx context.Context, tx sql.Transaction, prep evaluate.PreparedPlan,
	params []sql.CExpr) error {

	vals := make([]sql.Value, 0, len(params))
	for _, param := range params {
		val, err := param.Eval(ctx, tx, nil)
		if err != nil {
			return err
		}
		vals = append(vals, val)
	}
	return prep.SetParameters(vals)
}

type executeStmtPlan struct {
	params   []sql.CExpr
	prepStmt *evaluate.PreparedStmtPlan
}

func (_ executeStmtPlan) Tag() string {
	return "EXECUTE"
}

func (esp executeStmtPlan) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	err := setParameters(ctx, tx, esp.prepStmt, esp.params)
	if err != nil {
		return -1, err
	}
	return esp.prepStmt.Execute(ctx, tx)
}

type executeRowsPlan struct {
	params   []sql.CExpr
	prepRows *evaluate.PreparedRowsPlan
}

func (_ executeRowsPlan) Tag() string {
	return "EXECUTE"
}

func (erp executeRowsPlan) Columns() []sql.Identifier {
	return erp.prepRows.Columns()
}

func (erp executeRowsPlan) Rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	err := setParameters(ctx, tx, erp.prepRows, erp.params)
	if err != nil {
		return nil, err
	}
	return erp.prepRows.Rows(ctx, tx)
}
