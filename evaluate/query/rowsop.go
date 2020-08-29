package query

import (
	"context"

	"github.com/leftmike/maho/sql"
)

type rowsOp interface {
	explain() string
	rows(ctx context.Context, e sql.Engine, tx sql.Transaction) (sql.Rows, error)
}
