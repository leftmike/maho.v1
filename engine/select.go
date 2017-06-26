package engine

import (
	"fmt"
	"maho/sql/stmt"
	"maho/store"
)

func (e *Engine) Select(stmt *stmt.Select) (store.Rows, error) {
	fmt.Println(stmt)
	tbl, err := e.lookupTable(stmt.Table.Database, stmt.Table.Table)
	if err != nil {
		return nil, err
	}

	return tbl.Rows()
}
