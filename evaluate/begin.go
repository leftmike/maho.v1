package evaluate

import (
	"context"

	"github.com/leftmike/maho/sql"
)

type Begin struct{}

func (stmt *Begin) String() string {
	return "BEGIN"
}

func (stmt *Begin) Plan(ses *Session, ctx context.Context, pe PlanEngine,
	tx sql.Transaction) (Plan, error) {

	panic("do not call; begin handled by session")
	return nil, nil
}
