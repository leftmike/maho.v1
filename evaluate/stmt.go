package evaluate

import (
	"fmt"

	"github.com/leftmike/maho/sql"
)

type Stmt interface {
	fmt.Stringer
	Plan(ses *Session, tx sql.Transaction) (interface{}, error)
}
