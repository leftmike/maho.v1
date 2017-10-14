package db

import (
	"maho/sql"
)

type Rows interface {
	Columns() []sql.Identifier
	Close() error
	Next(dest []sql.Value) error
}

/*
XXX: remove from store/basic and store/test as well
type RowsColumnType interface {
	Rows
	ColumnTypes() []ColumnType
}
*/
