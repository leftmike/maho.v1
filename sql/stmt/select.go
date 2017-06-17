package stmt

import (
	"fmt"
	"maho/sql"
)

type Select struct {
	Database sql.Identifier
	Table    sql.Identifier
}

func (stmt *Select) String() string {
	return fmt.Sprintf("SELECT * FROM %s.%s", stmt.Database, stmt.Table)
}

func (stmt *Select) Dispatch(e Executer) (interface{}, error) {
	return e.Select(stmt)
}
