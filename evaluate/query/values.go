package query

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

type Values struct {
	Expressions [][]sql.Expr
}

// values implements the evaluate.Rows interface.
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
			s += v.String()
		}

		s += ")"
	}

	return s
}

func (stmt *Values) Plan(ses *evaluate.Session, tx *engine.Transaction) (interface{}, error) {
	return stmt.Rows(tx)
}

func (stmt *Values) Rows(tx *engine.Transaction) (evaluate.Rows, error) {
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

	return &values{columns: columns, rows: rows}, nil
}

func (v *values) Columns() []sql.Identifier {
	return v.columns
}

func (v *values) Close() error {
	v.index = len(v.rows)
	return nil
}

func (v *values) Next(ses *evaluate.Session, dest []sql.Value) error {
	if v.index == len(v.rows) {
		return io.EOF
	}
	copy(dest, v.rows[v.index])
	v.index += 1
	return nil
}

func (_ *values) Delete(ses *evaluate.Session) error {
	return fmt.Errorf("values rows may not be deleted")
}

func (_ *values) Update(ses *evaluate.Session, updates []sql.ColumnUpdate) error {
	return fmt.Errorf("values rows may not be updated")
}

type FromValues struct {
	Values
	Alias         sql.Identifier
	ColumnAliases []sql.Identifier
}

func (fv FromValues) String() string {
	s := fmt.Sprintf("(%s) AS %s", fv.Values.String(), fv.Alias)
	if fv.ColumnAliases != nil {
		s += " ("
		for i, col := range fv.ColumnAliases {
			if i > 0 {
				s += ", "
			}
			s += col.String()
		}
		s += ")"
	}
	return s
}

func (fv FromValues) rows(ses *evaluate.Session, tx *engine.Transaction) (evaluate.Rows,
	*fromContext, error) {

	rows, err := fv.Values.Rows(tx)
	if err != nil {
		return nil, nil, err
	}
	cols := rows.Columns()
	if fv.ColumnAliases != nil {
		if len(fv.ColumnAliases) != len(cols) {
			return nil, nil, fmt.Errorf("engine: wrong number of column aliases")
		}
		cols = fv.ColumnAliases
	}
	return rows, makeFromContext(fv.Alias, cols), nil
}

// TestRows is used for testing.
func (fv FromValues) TestRows(ses *evaluate.Session, tx *engine.Transaction) (evaluate.Rows,
	*fromContext, error) {

	return fv.rows(ses, tx)
}
