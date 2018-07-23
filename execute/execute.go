package execute

import (
	"fmt"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
)

type Stmt interface {
	fmt.Stringer
	Plan(ses *Session, tx *engine.Transaction) (interface{}, error)
}

type Rows db.Rows

type Executor interface {
	Execute(ses *Session, tx *engine.Transaction) (int64, error)
}
