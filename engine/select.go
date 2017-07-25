package engine

import (
	"fmt"
	"maho/sql/stmt"
)

func (e *Engine) Select(stmt *stmt.Select) (interface{}, error) {
	fmt.Println(stmt)
	tbl, err := e.lookupTable(stmt.Table.Database, stmt.Table.Table)
	if err != nil {
		return nil, err
	}

	return tbl.Rows()
}
