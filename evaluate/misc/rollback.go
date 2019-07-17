package misc

import (
	"context"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
)

type Rollback struct{}

func (stmt *Rollback) String() string {
	return "ROLLBACK"
}

func (stmt *Rollback) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	return rollbackPlan{ses}, nil
}

type rollbackPlan struct {
	ses *evaluate.Session
}

func (plan rollbackPlan) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	return -1, plan.ses.Rollback()
}
