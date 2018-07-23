package datadef

import (
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/execute"
	"github.com/leftmike/maho/sql"
)

type DetachDatabase struct {
	Database sql.Identifier
}

func (stmt *DetachDatabase) String() string {
	return fmt.Sprintf("DETACH DATABASE %s", stmt.Database)
}

func (stmt *DetachDatabase) Plan(ses *execute.Session, tx *engine.Transaction) (interface{},
	error) {

	return stmt, nil
}

func (stmt *DetachDatabase) Execute(ses *execute.Session, tx *engine.Transaction) (int64, error) {
	return -1, ses.DetachDatabase(stmt.Database)
}
