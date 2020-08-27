package evaluate

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/sql"
)

type Stmt interface {
	fmt.Stringer
	Plan(ses *Session, tx sql.Transaction) (Plan, error)
}

type Plan interface {
	Explain() string
}

type StmtPlan interface {
	Plan
	Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64, error)
}

type CmdPlan interface {
	Plan
	Command(ses *Session) error
}

type RowsPlan interface {
	Plan
	Rows(ctx context.Context, e sql.Engine, tx sql.Transaction) (sql.Rows, error)
}
