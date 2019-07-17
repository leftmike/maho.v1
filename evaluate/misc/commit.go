package misc

import (
	"context"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
)

type Commit struct{}

func (stmt *Commit) String() string {
	return "COMMIT"
}

func (stmt *Commit) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	return commitPlan{ses}, nil
}

type commitPlan struct {
	ses *evaluate.Session
}

func (plan commitPlan) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	return -1, plan.ses.Commit()
}
