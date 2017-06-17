package engine

import (
	"fmt"
	"maho/sql/stmt"
	"maho/store"
)

func (e *Engine) Select(stmt *stmt.Select) (store.Rows, error) {
	fmt.Println(stmt)
	id := stmt.Database
	if id == 0 {
		id = e.defaultDatabase
	}
	db, ok := e.databases[id]
	if !ok {
		return nil, fmt.Errorf("engine: database \"%s\" not found", id)
	}
	tbl, err := db.store.Table(stmt.Table)
	if err != nil {
		return nil, err
	}

	return tbl.Rows()
}
