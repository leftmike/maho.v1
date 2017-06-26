package engine

import (
	"fmt"
	"maho/sql"
	"maho/sql/stmt"
)

func (e *Engine) InsertValues(stmt *stmt.InsertValues) (interface{}, error) {
	fmt.Println(stmt)

	tbl, err := e.lookupTable(stmt.Table.Database, stmt.Table.Table)
	if err != nil {
		return nil, err
	}

	cols := tbl.Columns()
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

		mv = len(stmt.Columns)
		cmap := tbl.ColumnMap()
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
		for i, c := range cols {
			var v sql.Value
			if c2v[i] < len(r) {
				v = r[c2v[i]]
			} else {
				v = sql.Default{}
			}

			row[i], err = c.ConvertValue(v)
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
