package plan

import (
	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
)

type Rows db.Rows

type Executer interface {
	Execute(e *engine.Engine) (int64, error)
}
