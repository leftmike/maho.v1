package query

import (
	"context"

	"github.com/leftmike/maho/sql"
)

type rowsOp interface {
	explain() string
	children() []rowsOp
	rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error)
}

type resultRowsOp interface {
	rowsOp
	columns() []sql.Identifier
}
