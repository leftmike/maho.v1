package misc

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Set struct {
	Variable sql.Identifier
	Value    string
}

func (stmt *Set) String() string {
	return fmt.Sprintf("SET %s TO %s", stmt.Variable, stmt.Value)
}

func (_ *Set) Resolve(ses *evaluate.Session) {}

func (stmt *Set) Plan(ctx context.Context, pctx evaluate.PlanContext) (evaluate.Plan, error) {
	return stmt, nil
}

func (stmt *Set) Explain() string {
	return stmt.String()
}

func (stmt *Set) Command(ctx context.Context, ses *evaluate.Session) error {
	return ses.Set(stmt.Variable, stmt.Value)
}
