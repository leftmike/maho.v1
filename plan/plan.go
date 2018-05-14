package plan

import (
	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/session"
)

type Rows db.Rows

type Executer interface {
	Execute(ctx session.Context, tx engine.Transaction) (int64, error)
}
