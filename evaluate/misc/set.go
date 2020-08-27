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

func (stmt *Set) Plan(ses *evaluate.Session, ctx context.Context, pe evaluate.PlanEngine,
	tx sql.Transaction) (evaluate.Plan, error) {

	return stmt, nil
}

func (stmt *Set) Explain() string {
	return stmt.String()
}

func (stmt *Set) Command(ses *evaluate.Session) error {
	return ses.Set(stmt.Variable, stmt.Value)
}
