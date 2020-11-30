package misc

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Prepare struct {
	Name sql.Identifier
	Stmt evaluate.Stmt
	prep evaluate.PreparedPlan
}

func (stmt *Prepare) String() string {
	return fmt.Sprintf("PREPARE %s AS %s", stmt.Name, stmt.Stmt)
}

func (stmt *Prepare) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction, cctx sql.CompileContext) (evaluate.Plan, error) {

	var err error
	stmt.prep, err = evaluate.PreparePlan(ctx, stmt.Stmt, pctx, tx)
	if err != nil {
		return nil, err
	}

	return stmt, nil
}

func (stmt *Prepare) Tag() string {
	return "PREPARE"
}

func (stmt *Prepare) Command(ctx context.Context, ses *evaluate.Session, e sql.Engine) error {
	return ses.SetPreparedPlan(stmt.Name, stmt.prep)
}
