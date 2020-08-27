package evaluate

import (
	"github.com/leftmike/maho/sql"
)

type Begin struct{}

func (stmt *Begin) String() string {
	return "BEGIN"
}

func (stmt *Begin) Plan(ses *Session, tx sql.Transaction) (Plan, error) {
	panic("do not call; begin handled by session")
	return nil, nil
}
