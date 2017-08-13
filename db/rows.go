package db

import (
	"maho/sql"
)

type Rows interface {
	Columns() []ColumnType
	Close() error
	Next(dest []sql.Value) error
}
