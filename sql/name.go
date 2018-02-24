package sql

import (
	"fmt"
)

type TableName struct {
	Database Identifier
	Table    Identifier
}

type TableAlias struct {
	Database Identifier
	Table    Identifier
	Alias    Identifier
}

func (tn TableName) String() string {
	if tn.Database == 0 {
		return tn.Table.String()
	}
	return fmt.Sprintf("%s.%s", tn.Database, tn.Table)
}

func (ta TableAlias) String() string {
	var s string
	if ta.Database == 0 {
		s = ta.Table.String()
	} else {
		s = fmt.Sprintf("%s.%s", ta.Database, ta.Table)
	}
	if ta.Alias != 0 {
		s += fmt.Sprintf(" AS %s", ta.Alias)
	}
	return s
}
