package misc

import (
	"context"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Commit struct{}

func (_ *Commit) String() string {
	return "COMMIT"
}

func (stmt *Commit) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction, cctx sql.CompileContext) (evaluate.Plan, error) {

	return stmt, nil
}

func (_ *Commit) Tag() string {
	return "COMMIT"
}

func (_ *Commit) Command(ctx context.Context, ses *evaluate.Session, e sql.Engine) error {
	return ses.Commit()
}
