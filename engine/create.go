package engine

import (
	"fmt"
	"maho/sql/stmt"
)

func (e *Engine) CreateTable(stmt *stmt.CreateTable) (interface{}, error) {
	fmt.Println(stmt)
	id := stmt.Table.Database
	if id == 0 {
		id = e.defaultDatabase
	}
	db, ok := e.databases[id]
	if !ok {
		return nil, fmt.Errorf("engine: database \"%s\" not found", id)
	}
	return nil, db.CreateTable(stmt.Table.Table, stmt.Columns)
}
