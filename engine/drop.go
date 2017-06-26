package engine

import (
	"fmt"
	"maho/sql/stmt"
)

func (e *Engine) DropTable(stmt *stmt.DropTable) (interface{}, error) {
	fmt.Println(stmt)
	for _, tbl := range stmt.Tables {
		id := tbl.Database
		if id == 0 {
			id = e.defaultDatabase
		}
		db, ok := e.databases[id]
		if !ok {
			return nil, fmt.Errorf("engine: database \"%s\" not found", id)
		}
		err := db.DropTable(tbl.Table)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}
