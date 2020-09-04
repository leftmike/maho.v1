package misc

import (
	"context"
	"fmt"

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

func (stmt *Set) Plan(ctx context.Context, ses *evaluate.Session,
	tx sql.Transaction) (evaluate.Plan, error) {

	return stmt, nil
}

func (_ *Set) Planned() {}

func (stmt *Set) Command(ctx context.Context, ses *evaluate.Session, e sql.Engine) error {
	return ses.Set(stmt.Variable, stmt.Value)
}
