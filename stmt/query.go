package stmt

import (
	"fmt"

	"maho/engine"
	"maho/expr"
	"maho/query"
	"maho/sql"
)

type Select query.Select

func (stmt *Select) String() string {
	return (*query.Select)(stmt).String()
}

func (stmt *Select) Execute(e *engine.Engine) (interface{}, error) {
	fmt.Println(stmt)
	return (*query.Select)(stmt).Rows(e)
}

type Values query.Values

func (stmt *Values) String() string {
	return (*query.Values)(stmt).String()
}

func (stmt *Values) Execute(e *engine.Engine) (interface{}, error) {
	fmt.Println(stmt)
	return (*query.Values)(stmt).Rows(e)
}

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

func (stmt *Update) Execute(e *engine.Engine) (interface{}, error) {
	fmt.Println(stmt)

	return nil, fmt.Errorf("UPDATE: not implemented yet")
}