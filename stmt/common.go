package stmt

import (
	"fmt"
	"maho/sql"
)

type TableName struct {
	Database sql.Identifier
	Table    sql.Identifier
}

func (tn TableName) String() string {
	if tn.Database == 0 {
		return tn.Table.String()
	}
	return fmt.Sprintf("%s.%s", tn.Database, tn.Table)
}

type TableAlias struct {
	TableName
	Alias sql.Identifier
}

func (ta TableAlias) String() string {
	s := ta.TableName.String()
	if ta.Table != ta.Alias {
		s += fmt.Sprintf(" AS %s", ta.Alias)
	}
	return s
}
