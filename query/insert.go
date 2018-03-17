package query

import (
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/sql"
)

type InsertValues struct {
	Table   sql.TableName
	Columns []sql.Identifier
	Rows    [][]expr.Expr
}

func (stmt *InsertValues) String() string {
	s := fmt.Sprintf("INSERT INTO %s ", stmt.Table)
	if stmt.Columns != nil {
		s += "("
		for i, col := range stmt.Columns {
			if i > 0 {
				s += ", "
			}
			s += col.String()
		}
		s += ") "
	}

	s += "VALUES"

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
			s += v.String()
		}

		s += ")"
	}

	return s
}

func (stmt *InsertValues) Plan(tx engine.Transaction) (interface{}, error) {
	return stmt, nil
}

func (stmt *InsertValues) Execute(tx engine.Transaction) (int64, error) {
	tbl, err := engine.LookupTable(tx, stmt.Table.Database, stmt.Table.Table)
	if err != nil {
		return 0, err
	}

	cols := tbl.Columns()
	colTypes := tbl.ColumnTypes()
	mv := len(cols)
	c2v := make([]int, mv) // column number to value number
	if stmt.Columns == nil {
		for c := range c2v {
			c2v[c] = c
		}
	} else {
		for c := range c2v {
			c2v[c] = len(c2v)
		}

		var cmap = make(map[sql.Identifier]int)
		for i, cn := range cols {
			cmap[cn] = i
		}

		mv = len(stmt.Columns)
		for v, nam := range stmt.Columns {
			c, ok := cmap[nam]
			if !ok {
				return 0, fmt.Errorf("engine: %s: column not found: %s", stmt.Table, nam)
			}
			c2v[c] = v
		}
	}

	for _, r := range stmt.Rows {
		if len(r) > mv {
			return 0, fmt.Errorf("engine: %s: too many values", stmt.Table)
		}
		row := make([]sql.Value, len(cols))
		for i, c := range colTypes {
			e := c.Default
			if c2v[i] < len(r) {
				e = r[c2v[i]]
				if e == nil {
					e = c.Default
				}
			}
			var v sql.Value
			if e != nil {
				ce, err := expr.Compile(nil, e, false)
				if err != nil {
					return 0, err
				}
				v, err = ce.Eval(nil)
				if err != nil {
					return 0, err
				}
			}

			row[i], err = c.ConvertValue(cols[i], v)
			if err != nil {
				return 0, fmt.Errorf("engine: table %s: %s", stmt.Table, err.Error())
			}
		}

		err := tbl.Insert(row)
		if err != nil {
			return 0, err
		}
	}

	return int64(len(stmt.Rows)), nil
}
