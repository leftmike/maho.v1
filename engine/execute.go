package engine

import (
	"fmt"
	"maho/sql"
	"maho/sql/stmt"
)

func (e *Engine) CreateTable(stmt *stmt.CreateTable) (interface{}, error) {
	fmt.Println(stmt)
	id := stmt.Database
	if id == 0 {
		id = e.defaultDatabase
	}
	db, ok := e.databases[id]
	if !ok {
		return nil, fmt.Errorf("engine: database \"%s\" not found", id)
	}
	return db.store.CreateTable(stmt.Table, stmt.Columns), nil
}

func (e *Engine) InsertValues(stmt *stmt.InsertValues) (interface{}, error) {
	fmt.Println(stmt)

	tbl, err := e.lookupTable(stmt.Database, stmt.Table)
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
				if _, ok := v.(sql.Default); ok {
					v = nil
				}
			}
			if v == nil {
				if c.NotNull && c.Default == nil {
					return nil, fmt.Errorf("engine: %s: value must be specified for column: %s",
						tbl.Name(), c.Name)
				} else if c.Default == nil {
					v = sql.Null{}
				} else {
					v = c.Default
				}
			}
			row[i] = v
		}

		err := tbl.Insert(row)
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}
