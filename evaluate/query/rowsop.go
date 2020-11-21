package query

import (
	"context"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type rowsOp interface {
	evaluate.ExplainTree
	rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error)
}

type resultRowsOp interface {
	rowsOp
	columns() []sql.Identifier
	columnTypes() []sql.ColumnType
}
