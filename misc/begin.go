package misc

import (
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/execute"
	"github.com/leftmike/maho/session"
)

type Begin struct{}

func (stmt *Begin) String() string {
	return "BEGIN"
}

func (stmt *Begin) Plan(ctx session.Context, tx *engine.Transaction) (execute.Plan, error) {
	return stmt, nil
}

func (stmt *Begin) Execute(ctx session.Context, tx *engine.Transaction) (int64, error) {
	return 0, nil // XXX
}
