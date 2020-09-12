package misc

import (
	"context"
	"errors"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Show struct {
	Variable sql.Identifier
	ses      *evaluate.Session
}

func (stmt *Show) String() string {
	return fmt.Sprintf("SHOW %s", stmt.Variable)
}

func (stmt *Show) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction) (evaluate.Plan, error) {

	ses, ok := pctx.(*evaluate.Session)
	if !ok {
		return nil, errors.New("engine: show not allowed here")
	}
	stmt.ses = ses
	return stmt, nil
}

func (_ *Show) Planned() {}

func (stmt *Show) Columns() []sql.Identifier {
	return stmt.ses.Columns(stmt.Variable)
}

func (stmt *Show) Rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	return stmt.ses.Show(stmt.Variable)
}
