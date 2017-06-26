package stmt

import (
	"fmt"
	"maho/sql"
)

type TableName struct {
	Database sql.Identifier
	Table    sql.Identifier
}

func (tn *TableName) String() string {
	if tn.Database == 0 {
		return fmt.Sprintf("%s ", tn.Table)
	}
	return fmt.Sprintf("%s.%s ", tn.Database, tn.Table)
}
