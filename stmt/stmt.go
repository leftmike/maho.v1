package stmt

import (
	"fmt"

	"github.com/leftmike/maho/engine"
)

type Stmt interface {
	fmt.Stringer
	Plan(e *engine.Engine) (interface{}, error)
}
