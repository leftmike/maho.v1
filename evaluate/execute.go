package evaluate

import (
	"context"

	"github.com/leftmike/maho/engine"
)

type Executor interface {
	Execute(ctx context.Context, e *engine.Engine, tx engine.Transaction) (int64, error)
}
