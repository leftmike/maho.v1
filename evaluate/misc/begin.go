package misc

import (
	"context"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
)

type Begin struct{}

func (stmt *Begin) String() string {
	return "BEGIN"
}

func (stmt *Begin) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	return beginPlan{ses}, nil
}

type beginPlan struct {
	ses *evaluate.Session
}

func (plan beginPlan) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	return -1, plan.ses.Begin()
}
