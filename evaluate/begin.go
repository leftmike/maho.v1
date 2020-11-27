package evaluate

import (
	"context"

	"github.com/leftmike/maho/sql"
)

type Begin struct{}

func (stmt *Begin) String() string {
	return "BEGIN"
}

func (stmt *Begin) Plan(ctx context.Context, pctx PlanContext, tx sql.Transaction) (Plan, error) {
	return stmt, nil
}

func (_ *Begin) Tag() string {
	return "BEGIN"
}

func (_ *Begin) Command(ctx context.Context, ses *Session, e sql.Engine) error {
	return ses.Begin()
}
