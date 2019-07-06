package misc

import (
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
	return stmt, nil
}

func (stmt *Set) Execute(ses *evaluate.Session, tx engine.Transaction) (int64, error) {
	return -1, ses.Set(stmt.Variable, stmt.Value)
}
