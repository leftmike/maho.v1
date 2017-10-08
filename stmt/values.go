package stmt

import (
	"fmt"
	"io"

	"maho/engine"
	"maho/expr"
	"maho/sql"
)

type Values struct {
	Rows [][]expr.Expr
}

// values implements the db.Rows interface.
type values struct {
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
}

func (stmt *Values) String() string {
	s := "VALUES"
	for i, r := range stmt.Rows {
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

func (stmt *Values) Execute(e *engine.Engine) (interface{}, error) {
	fmt.Println(stmt)

	columns := make([]sql.Identifier, len(stmt.Rows[0]))
	for i := 0; i < len(columns); i++ {
		columns[i] = sql.ID(fmt.Sprintf("column-%d", i+1))
	}

	rows := make([][]sql.Value, len(stmt.Rows))
	for i, r := range stmt.Rows {
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
