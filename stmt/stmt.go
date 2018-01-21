package stmt

import (
	"fmt"

	"github.com/leftmike/maho/engine"
)

type Stmt interface {
	fmt.Stringer
	Execute(e *engine.Engine) (interface{}, error)
}
