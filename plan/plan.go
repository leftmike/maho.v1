package plan

import (
	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
)

type Rows db.Rows

type Executer interface {
	Execute(tx engine.Transaction) (int64, error)
}
