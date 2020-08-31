package misc

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Show struct {
	Variable sql.Identifier
	ses      *evaluate.Session
}

func (stmt *Show) String() string {
	return fmt.Sprintf("SHOW %s", stmt.Variable)
}

func (stmt *Show) Resolve(ses *evaluate.Session) {
	stmt.ses = ses
}

func (stmt *Show) Plan(ctx context.Context, pctx evaluate.PlanContext) (evaluate.Plan, error) {
	return stmt, nil
}

func (stmt *Show) Explain() string {
	return stmt.String()
}

func (stmt *Show) Columns() []sql.Identifier {
	return stmt.ses.Columns(stmt.Variable)
}

func (stmt *Show) Rows(ctx context.Context, e sql.Engine, tx sql.Transaction) (sql.Rows, error) {
	return stmt.ses.Show(stmt.Variable)
}
