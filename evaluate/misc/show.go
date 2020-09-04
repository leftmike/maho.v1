package misc

import (
	"context"
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

func (stmt *Show) Plan(ctx context.Context, ses *evaluate.Session,
	tx sql.Transaction) (evaluate.Plan, error) {

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
