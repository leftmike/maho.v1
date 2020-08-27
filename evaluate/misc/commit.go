package misc

import (
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Commit struct{}

func (stmt *Commit) String() string {
	return "COMMIT"
}

func (stmt *Commit) Plan(ses *evaluate.Session, tx sql.Transaction) (evaluate.Plan, error) {
	return stmt, nil
}

func (stmt *Commit) Explain() string {
	return stmt.String()
}

func (stmt *Commit) Command(ses *evaluate.Session) error {
	return ses.Commit()
}
