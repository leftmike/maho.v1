package misc

import (
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Commit struct{}

func (stmt *Commit) String() string {
	return "COMMIT"
}

func (stmt *Commit) Plan(ses *evaluate.Session, tx sql.Transaction) (interface{}, error) {
	return stmt, nil
}

func (stmt *Commit) Command(ses *evaluate.Session) error {
	return ses.Commit()
}
