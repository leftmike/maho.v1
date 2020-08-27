package misc

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/query"
	"github.com/leftmike/maho/sql"
)

type Show struct {
	Variable sql.Identifier
}

func (stmt *Show) String() string {
	return fmt.Sprintf("SHOW %s", stmt.Variable)
}

func (stmt *Show) Plan(ses *evaluate.Session, ctx context.Context, pe evaluate.PlanEngine,
	tx sql.Transaction) (evaluate.Plan, error) {

	return query.RowsPlan(ses.Show(stmt.Variable))
}
