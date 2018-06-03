package misc

import (
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/execute"
)

type Begin struct{}

func (stmt *Begin) String() string {
	return "BEGIN"
}

func (stmt *Begin) Plan(ses execute.Session, tx *engine.Transaction) (execute.Plan, error) {
	return stmt, nil
}

func (stmt *Begin) Execute(ses execute.Session, tx *engine.Transaction) (int64, error) {
	return 0, nil // XXX
}
