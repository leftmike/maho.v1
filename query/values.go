package query

import (
	"fmt"
	"io"

	"maho/db"
	"maho/engine"
	"maho/expr"
	"maho/sql"
)

type Values struct {
	Expressions [][]expr.Expr
}

// values implements the db.Rows interface.
type values struct {
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
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
			s += sql.Format(v)
		}

		s += ")"
	}

	return s
}

func (stmt *Values) Rows(e *engine.Engine) (db.Rows, error) {
	columns := make([]sql.Identifier, len(stmt.Expressions[0]))
	for i := 0; i < len(columns); i++ {
		columns[i] = sql.ID(fmt.Sprintf("column-%d", i+1))
	}

	rows := make([][]sql.Value, len(stmt.Expressions))
	for i, r := range stmt.Expressions {
		row := make([]sql.Value, len(r))
		for j := range r {
			ce, err := expr.Compile(nil, r[j])
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

	return &values{columns: columns, rows: rows}, nil
}

func (v *values) Columns() []sql.Identifier {
	return v.columns
}

func (v *values) Close() error {
	v.index = len(v.rows)
	return nil
}

func (v *values) Next(dest []sql.Value) error {
	if v.index == len(v.rows) {
		return io.EOF
	}
	copy(dest, v.rows[v.index])
	v.index += 1
	return nil
}
