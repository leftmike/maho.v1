package evaluate

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/flags"
	"github.com/leftmike/maho/sql"
)

type Stmt interface {
	fmt.Stringer
	Plan(ctx context.Context, pctx PlanContext, tx sql.Transaction) (Plan, error)
}

type PlanContext interface {
	GetFlag(f flags.Flag) bool
	ResolveTableName(tn sql.TableName) sql.TableName
	ResolveSchemaName(sn sql.SchemaName) sql.SchemaName
	PlanParameter(num int) (*sql.Value, error)
	GetPreparedPlan(nam sql.Identifier) PreparedPlan
}

type Plan interface {
	Tag() string
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
	ColumnTypes() []sql.ColumnType
	Rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error)
}

type FieldDescription struct {
	Field       string
	Description string
}

type ExplainTree interface {
	Name() string
	Columns() []string
	Fields() []FieldDescription
	Children() []ExplainTree
}
