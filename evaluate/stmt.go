package evaluate

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/sql"
)

type Stmt interface {
	fmt.Stringer
	Resolve(ses *Session)
	Plan(ctx context.Context, tx sql.Transaction) (Plan, error)
}

type Plan interface {
	Explain() string
}

type StmtPlan interface {
	Plan
	Execute(ctx context.Context, tx sql.Transaction) (int64, error)
}

type CmdPlan interface {
	Plan
	Command(ctx context.Context, ses *Session, e sql.Engine) error
}

type RowsPlan interface {
	Plan
	Columns() []sql.Identifier
	Rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error)
}
