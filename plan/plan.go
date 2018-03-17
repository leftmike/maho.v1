package plan

import (
	"context"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
)

type Rows db.Rows

type Executer interface {
	Execute(ctx context.Context, tx engine.Transaction) (int64, error)
}
