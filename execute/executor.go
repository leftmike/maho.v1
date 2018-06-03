package execute

import (
	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
)

type Rows db.Rows

type Executor interface {
	Execute(ses Session, tx *engine.Transaction) (int64, error)
}

type Plan interface{}
