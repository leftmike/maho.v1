package misc

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Set struct {
	Variable sql.Identifier
	Value    string
}

func (stmt *Set) String() string {
	return fmt.Sprintf("SET %s TO %s", stmt.Variable, stmt.Value)
}

func (stmt *Set) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	return &setPlan{ses, stmt.Variable, stmt.Value}, nil
}

type setPlan struct {
	ses      *evaluate.Session
	variable sql.Identifier
	value    string
}

func (plan setPlan) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	return -1, plan.ses.Set(plan.variable, plan.value)
}
