package misc

import (
	"fmt"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/execute"
	"github.com/leftmike/maho/sql"
)

type Set struct {
	Variable sql.Identifier
	Value    string
}

func (stmt *Set) String() string {
	return fmt.Sprintf("SET %s TO %s", stmt.Variable, stmt.Value)
}

func (stmt *Set) Plan(ses execute.Session, tx *engine.Transaction) (execute.Plan, error) {
	return stmt, nil
}

func (stmt *Set) Execute(ses execute.Session, tx *engine.Transaction) (int64, error) {
	return 0, config.Set(stmt.Variable.String(), stmt.Value)
}
