package stmt

import (
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/query"
	"github.com/leftmike/maho/sql"
)

type Select query.Select

func (stmt *Select) String() string {
	return (*query.Select)(stmt).String()
}

func (stmt *Select) Plan(e *engine.Engine) (interface{}, error) {
	return (*query.Select)(stmt).Rows(e)
}

type Values query.Values

func (stmt *Values) String() string {
	return (*query.Values)(stmt).String()
}

func (stmt *Values) Plan(e *engine.Engine) (interface{}, error) {
	return (*query.Values)(stmt).Rows(e)
}

type Delete query.Delete

func (stmt *Delete) String() string {
	return (*query.Delete)(stmt).String()
}

func (stmt *Delete) Plan(e *engine.Engine) (interface{}, error) {
	return (*query.Delete)(stmt).Plan(e)
}

type ColumnUpdate struct {
	Column sql.Identifier
	Expr   expr.Expr
}

type Update struct {
	Table         TableName
	ColumnUpdates []ColumnUpdate
	Where         expr.Expr
}

func (stmt *Update) String() string {
	s := fmt.Sprintf("UPDATE %s SET ", stmt.Table)
	for i, cu := range stmt.ColumnUpdates {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s = %s", cu.Column, cu.Expr)
	}
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

func (stmt *Update) Plan(e *engine.Engine) (interface{}, error) {
	return nil, fmt.Errorf("UPDATE: not implemented yet")
}
