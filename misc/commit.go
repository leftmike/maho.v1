package misc

import (
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/execute"
	"github.com/leftmike/maho/session"
)

type Commit struct{}

func (stmt *Commit) String() string {
	return "COMMIT"
}

func (stmt *Commit) Plan(ctx session.Context, tx *engine.Transaction) (execute.Plan, error) {
	return stmt, nil
}

func (stmt *Commit) Execute(ctx session.Context, tx *engine.Transaction) (int64, error) {
	return 0, nil // XXX
}
