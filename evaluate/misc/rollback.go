package misc

import (
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Rollback struct{}

func (stmt *Rollback) String() string {
	return "ROLLBACK"
}

func (stmt *Rollback) Plan(ses *evaluate.Session, tx sql.Transaction) (evaluate.Plan, error) {
	return stmt, nil
}

func (stmt *Rollback) Explain() string {
	return stmt.String()
}

func (stmt *Rollback) Command(ses *evaluate.Session) error {
	return ses.Rollback()
}
