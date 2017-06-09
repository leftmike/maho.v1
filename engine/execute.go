package engine

import (
	"errors"
	"fmt"
	"maho/sql/stmt"
)

var CanNotBeExecuted = errors.New("engine: can not be executed")

func Execute(s stmt.Stmt) error {
	switch stmt := s.(type) {
	case *stmt.CreateTable:
		fmt.Println(stmt)
		id := stmt.Database
		if id == 0 {
			id = defaultDatabase
		}
		db, ok := databases[id]
		if !ok {
			return fmt.Errorf("engine: database \"%s\" not found", id)
		}
		return db.store.CreateTable(stmt.Table, stmt.Columns)
	case *stmt.InsertValues:
		fmt.Println(stmt)

		return nil
	}

	return CanNotBeExecuted
}
