package engine

import (
	"fmt"
	"maho/sql"
	"maho/sql/stmt"
	"maho/store"
)

func (e *Engine) lookupTable(db, tbl sql.Identifier) (store.Table, error) {
	if db == 0 {
		db = e.defaultDatabase
	}
	database, ok := e.databases[db]
	if !ok {
		return nil, fmt.Errorf("engine: database \"%s\" not found", db)
	}
	return database.store.Table(tbl)
}

func (e *Engine) Select(stmt *stmt.Select) (store.Rows, error) {
	fmt.Println(stmt)
	tbl, err := e.lookupTable(stmt.Database, stmt.Table)
	if err != nil {
		return nil, err
	}

	return tbl.Rows()
}
