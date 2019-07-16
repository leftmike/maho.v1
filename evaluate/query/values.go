package query

import (
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

type Values struct {
	Expressions [][]sql.Expr
}

func (stmt *Values) String() string {
	s := "VALUES"
	for i, r := range stmt.Expressions {
		if i > 0 {
			s += ", ("
		} else {
			s += " ("
		}

		for j, v := range r {
			if j > 0 {
				s += ", "
			}
			s += v.String()
		}

		s += ")"
	}

	return s
}

func (stmt *Values) Plan(ses *evaluate.Session, tx engine.Transaction) (interface{}, error) {
	columns := make([]sql.Identifier, len(stmt.Expressions[0]))
	for i := 0; i < len(columns); i++ {
		columns[i] = sql.ID(fmt.Sprintf("column%d", i+1))
	}

	rows := make([][]sql.Value, len(stmt.Expressions))
	for i, r := range stmt.Expressions {
		row := make([]sql.Value, len(r))
		for j := range r {
			ce, err := expr.Compile(nil, r[j], false)
			if err != nil {
				return nil, err
			}
			row[j], err = ce.Eval(nil)
			if err != nil {
				return nil, err
			}
		}
		rows[i] = row
	}

	return &evaluate.Values{Cols: columns, Rows: rows}, nil
}
