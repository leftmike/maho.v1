package misc

import (
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/execute"
	"github.com/leftmike/maho/session"
)

type Rollback struct{}

func (stmt *Rollback) String() string {
	return "ROLLBACK"
}

func (stmt *Rollback) Plan(ctx session.Context, tx *engine.Transaction) (execute.Plan, error) {
	return stmt, nil
}

func (stmt *Rollback) Execute(ctx session.Context, tx *engine.Transaction) (int64, error) {
	return 0, nil // XXX
}
