package sql

import (
	"fmt"
)

type SchemaName struct {
	Database Identifier
	Schema   Identifier
}

type TableName struct {
	Database Identifier
	Schema   Identifier
	Table    Identifier
}

func (sn SchemaName) String() string {
	if sn.Database == 0 {
		return sn.Schema.String()
	}
	return fmt.Sprintf("%s.%s", sn.Database, sn.Schema)
}

func (tn TableName) String() string {
	if tn.Database == 0 {
		if tn.Schema == 0 {
			return tn.Table.String()
		}
		return fmt.Sprintf("%s.%s", tn.Schema, tn.Table)
	}
	return fmt.Sprintf("%s.%s.%s", tn.Database, tn.Schema, tn.Table)
}
