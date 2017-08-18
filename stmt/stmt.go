package stmt

import (
	"fmt"

	"maho/engine"
)

type Stmt interface {
	fmt.Stringer
	Execute(e *engine.Engine) (interface{}, error)
}
