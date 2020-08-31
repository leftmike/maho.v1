package misc

import (
	"context"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Commit struct{}

func (stmt *Commit) String() string {
	return "COMMIT"
}

func (_ *Commit) Resolve(ses *evaluate.Session) {}

func (stmt *Commit) Plan(ctx context.Context, pctx evaluate.PlanContext) (evaluate.Plan, error) {
	return stmt, nil
}

func (stmt *Commit) Explain() string {
	return stmt.String()
}

func (stmt *Commit) Command(ctx context.Context, ses *evaluate.Session, e sql.Engine) error {
	return ses.Commit()
}
