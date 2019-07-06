package evaluate

import (
	"github.com/leftmike/maho/engine"
)

type Executor interface {
	Execute(ses *Session, tx engine.Transaction) (int64, error)
}
