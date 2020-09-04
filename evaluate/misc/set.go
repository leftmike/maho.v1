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
	ses      *evaluate.Session
}

func (stmt *Set) String() string {
	return fmt.Sprintf("SET %s TO %s", stmt.Variable, stmt.Value)
}

func (stmt *Set) Plan(ctx context.Context, ses *evaluate.Session,
	tx sql.Transaction) (evaluate.Plan, error) {

	stmt.ses = ses
	return stmt, nil
}

func (_ *Set) Planned() {}

func (stmt *Set) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	return -1, stmt.ses.Set(stmt.Variable, stmt.Value)
}
