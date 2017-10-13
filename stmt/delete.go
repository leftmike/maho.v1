package stmt

import (
	"fmt"

	"maho/engine"
	"maho/expr"
)

type Delete struct {
	Table TableName
	Where expr.Expr
}

func (stmt *Delete) String() string {
	s := fmt.Sprintf("DELETE FROM %s", stmt.Table)
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

func (stmt *Delete) Execute(e *engine.Engine) (interface{}, error) {
	fmt.Println(stmt)

	return nil, fmt.Errorf("DELETE: not implemented yet")
}
