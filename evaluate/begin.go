package evaluate

import (
	"context"
)

type Begin struct{}

func (stmt *Begin) String() string {
	return "BEGIN"
}

func (_ *Begin) Resolve(ses *Session) {}

func (stmt *Begin) Plan(ctx context.Context, pctx PlanContext) (Plan, error) {
	panic("do not call; begin handled by session")
	return nil, nil
}
