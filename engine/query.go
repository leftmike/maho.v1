package engine

import (
	"errors"
	"fmt"
	"maho/sql/stmt"
	"maho/store"
)

var CanNotBeQueried = errors.New("engine: can not be queried")

func Query(s stmt.Stmt) (store.Rows, error) {
	switch stmt := s.(type) {
	case *stmt.Select:
		fmt.Println(stmt)
		id := stmt.Database
		if id == 0 {
			id = defaultDatabase
		}
		db, ok := databases[id]
		if !ok {
			return nil, fmt.Errorf("engine: database \"%s\" not found", id)
		}
		tbl, err := db.store.Table(stmt.Table)
		if err != nil {
			return nil, err
		}

		return tbl.Rows()
	}

	return nil, CanNotBeQueried
}
