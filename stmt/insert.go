package stmt

import (
	"fmt"

	"maho/db"
	"maho/engine"
	"maho/expr"
	"maho/sql"
)

type InsertValues struct {
	Table   TableName
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
			s += sql.Format(v)
		}

		s += ")"
	}

	return s
}

func (stmt *InsertValues) Execute(e *engine.Engine) (interface{}, error) {
	fmt.Println(stmt)

	dbase, err := e.LookupDatabase(stmt.Table.Database)
	if err != nil {
		return nil, err
	}
	t, err := dbase.Table(stmt.Table.Table)
	if err != nil {
		return nil, err
	}
	tbl, ok := t.(db.TableInsert)
	if !ok {
		return nil, fmt.Errorf("\"%s.%s\" table can't be modified", dbase.Name(), t.Name())
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
				return nil, fmt.Errorf("engine: %s: column not found: %s", tbl.Name(), nam)
			}
			c2v[c] = v
		}
	}

	for _, r := range stmt.Rows {
		if len(r) > mv {
			return nil, fmt.Errorf("engine: %s: too many values", tbl.Name())
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
				ce, err := expr.Compile(nil, e)
				if err != nil {
					return nil, err
				}
				v, err = ce.Eval(nil)
				if err != nil {
					return nil, err
				}
			}

			row[i], err = c.ConvertValue(cols[i], v)
			if err != nil {
				return nil, fmt.Errorf("%s: %s", tbl.Name(), err.Error())
			}
		}

		err := tbl.Insert(row)
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}
