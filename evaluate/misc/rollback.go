package misc

import (
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
)

type Rollback struct{}

func (stmt *Rollback) String() string {
	return "ROLLBACK"
}

func (stmt *Rollback) Plan(ses *evaluate.Session, tx *engine.Transaction) (interface{}, error) {
	return stmt, nil
}

func (stmt *Rollback) Execute(ses *evaluate.Session, tx *engine.Transaction) (int64, error) {
	return -1, ses.Rollback()
}
