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

func (_ *Rollback) Resolve(ses *evaluate.Session) {}

func (stmt *Rollback) Plan(ctx context.Context, pe evaluate.PlanEngine,
	tx sql.Transaction) (evaluate.Plan, error) {

	return stmt, nil
}

func (stmt *Rollback) Explain() string {
	return stmt.String()
}

func (stmt *Rollback) Command(ctx context.Context, ses *evaluate.Session) error {
	return ses.Rollback()
}
