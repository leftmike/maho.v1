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
	s := "SELECT * FROM "
	if stmt.Database == 0 {
		s += fmt.Sprintf("%s ", stmt.Table)
	} else {
		s += fmt.Sprintf("%s.%s ", stmt.Database, stmt.Table)
	}
	return s
}

func (stmt *Select) Dispatch(e Executer) (interface{}, error) {
	return e.Select(stmt)
}
