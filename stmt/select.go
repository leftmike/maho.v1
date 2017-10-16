package stmt

import (
	"fmt"

	"maho/db"
	"maho/engine"
	"maho/expr"
	"maho/sql"
)

type FromItem interface {
	fmt.Stringer
	Rows(e *engine.Engine) (db.Rows, error)
}

type SelectResult interface {
	fmt.Stringer
}

type TableResult struct {
	Table sql.Identifier
}

type TableColumnResult struct {
	Table  sql.Identifier
	Column sql.Identifier
	Alias  sql.Identifier
}

type ExprResult struct {
	Expr  expr.Expr
	Alias sql.Identifier
}

type Select struct {
	Results []SelectResult
	From    FromItem
	Where   expr.Expr
}

func (tr TableResult) String() string {
	return fmt.Sprintf("%s.*", tr.Table)
}

func (tcr TableColumnResult) String() string {
	var s string
	if tcr.Table == 0 {
		s = tcr.Column.String()
	} else {
		s = fmt.Sprintf("%s.%s", tcr.Table, tcr.Column)
	}
	if tcr.Alias != 0 {
		s += fmt.Sprintf(" AS %s", tcr.Alias)
	}
	return s
}

func (er ExprResult) String() string {
	s := er.Expr.String()
	if er.Alias != 0 {
		s += fmt.Sprintf(" AS %s", er.Alias)
	}
	return s
}

func (stmt *Select) String() string {
	s := "SELECT "
	if stmt.Results == nil {
		s += "*"
	} else {
		for i, sr := range stmt.Results {
			if i > 0 {
				s += ", "
			}
			s += sr.String()
		}
	}
	s += fmt.Sprintf(" FROM %s", stmt.From)
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

func (stmt *Select) Execute(e *engine.Engine) (interface{}, error) {
	fmt.Println(stmt)

	return stmt.Rows(e)
}

func (stmt *Select) Rows(e *engine.Engine) (db.Rows, error) {
	if stmt.From == nil {
		return nil, fmt.Errorf("SELECT with no FROM clause is not supported yet")
	}
	rows, err := stmt.From.Rows(e)
	if err != nil {
		return nil, err
	}
	if stmt.Where != nil {
		/*
			rows, err = query.Where(e, rows, stmt.Where)
			if err != nil {
				return nil, err
			}
		*/
		ctx, _ := rows.(expr.CompileContext) // XXX
		_, err := expr.Compile(ctx, stmt.Where)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}
