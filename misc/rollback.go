package misc

import (
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/execute"
)

type Rollback struct{}

func (stmt *Rollback) String() string {
	return "ROLLBACK"
}

func (stmt *Rollback) Plan(ses execute.Session, tx *engine.Transaction) (execute.Plan, error) {
	return stmt, nil
}

func (stmt *Rollback) Execute(ses execute.Session, tx *engine.Transaction) (int64, error) {
	return 0, nil // XXX
}
