package misc

import (
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/execute"
)

type Commit struct{}

func (stmt *Commit) String() string {
	return "COMMIT"
}

func (stmt *Commit) Plan(ses *execute.Session, tx *engine.Transaction) (interface{}, error) {
	return stmt, nil
}

func (stmt *Commit) Execute(ses *execute.Session, tx *engine.Transaction) (int64, error) {
	return -1, ses.Commit()
}
