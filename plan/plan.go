package plan

import (
	"github.com/leftmike/maho/db"
)

type Rows db.Rows

type Executer interface {
	Execute() (int64, error)
}
