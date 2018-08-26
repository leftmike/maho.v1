package evaluate

import (
	"fmt"

	"github.com/leftmike/maho/engine"
)

type Stmt interface {
	fmt.Stringer
	Plan(ses *Session, tx *engine.Transaction) (interface{}, error)
}
