package plan

import (
	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/oldeng"
)

type Rows db.Rows

type Executer interface {
	Execute(e *oldeng.Engine) (int64, error)
}
