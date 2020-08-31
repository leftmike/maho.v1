package evaluate

import (
	"context"

	"github.com/leftmike/maho/sql"
)

type Begin struct{}

func (stmt *Begin) String() string {
	return "BEGIN"
}

func (_ *Begin) Resolve(ses *Session) {}

func (stmt *Begin) Plan(ctx context.Context, tx sql.Transaction) (Plan, error) {
	panic("do not call; begin handled by session")
	return nil, nil
}
