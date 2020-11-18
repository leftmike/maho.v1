package misc

import (
	"context"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Rollback struct{}

func (stmt *Rollback) String() string {
	return "ROLLBACK"
}

func (stmt *Rollback) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction) (evaluate.Plan, error) {

	return stmt, nil
}

func (_ *Rollback) Tag() string {
	return "ROLLBACK"
}

func (stmt *Rollback) Command(ctx context.Context, ses *evaluate.Session, e sql.Engine) error {
	return ses.Rollback()
}
