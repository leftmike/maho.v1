package datadef

import (
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/execute"
	"github.com/leftmike/maho/session"
	"github.com/leftmike/maho/sql"
)

type DetachDatabase struct {
	Database sql.Identifier
}

func (stmt *DetachDatabase) String() string {
	return fmt.Sprintf("DETACH DATABASE %s", stmt.Database)
}

func (stmt *DetachDatabase) Plan(ctx session.Context, tx *engine.Transaction) (execute.Plan,
	error) {

	return stmt, nil
}

func (stmt *DetachDatabase) Execute(ctx session.Context, tx *engine.Transaction) (int64, error) {
	return 0, engine.DetachDatabase(stmt.Database)
}
