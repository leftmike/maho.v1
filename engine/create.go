package engine

import (
	"fmt"
	"maho/sql/stmt"
)

func (e *Engine) CreateTable(stmt *stmt.CreateTable) (interface{}, error) {
	fmt.Println(stmt)
	id := stmt.Database
	if id == 0 {
		id = e.defaultDatabase
	}
	s, ok := e.databases[id]
	if !ok {
		return nil, fmt.Errorf("engine: database \"%s\" not found", id)
	}
	return s.CreateTable(stmt.Table, stmt.Columns), nil
}
