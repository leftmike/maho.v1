package misc

import (
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Show struct {
	Variable sql.Identifier
}

func (stmt *Show) String() string {
	return fmt.Sprintf("SHOW %s", stmt.Variable)
}

func (stmt *Show) Plan(ses *evaluate.Session, tx sql.Transaction) (interface{}, error) {
	return ses.Show(stmt.Variable)
}
