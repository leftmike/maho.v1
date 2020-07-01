package evaluate

import (
	"context"

	"github.com/leftmike/maho/sql"
)

type Executor interface {
	Execute(ctx context.Context, e sql.Engine, tx sql.Transaction) (int64, error)
}
