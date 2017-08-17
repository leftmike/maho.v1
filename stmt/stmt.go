package stmt

import (
	"fmt"

	"maho/engine"
	"maho/sql"
)

type Stmt interface {
	fmt.Stringer
	Execute(e *engine.Engine) (interface{}, error)
}

type TableAlias struct {
	TableName
	Alias sql.Identifier
}
