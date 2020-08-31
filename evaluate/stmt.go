package evaluate

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/sql"
)

type Stmt interface {
	fmt.Stringer
	Resolve(ses *Session)
	Plan(ctx context.Context, pctx PlanContext) (Plan, error)
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
	Rows(ctx context.Context, e sql.Engine, tx sql.Transaction) (sql.Rows, error)
}

type PlanEngine sql.Engine

/*interface {
	LookupTableType(ctx context.Context, tx sql.Transaction, tn sql.TableName) (sql.TableType,
		error)
}*/

type PlanContext interface {
	Engine() PlanEngine
	Transaction() sql.Transaction
}

type planContext struct {
	pe PlanEngine
	tx sql.Transaction
}

func MakePlanContext(pe PlanEngine, tx sql.Transaction) PlanContext {
	return &planContext{
		pe: pe,
		tx: tx,
	}
}

func (pctx *planContext) Engine() PlanEngine {
	return pctx.pe
}

func (pctx *planContext) Transaction() sql.Transaction {
	return pctx.tx
}
