package db

import (
	"maho/sql"
)

type Table interface {
	Name() sql.Identifier
	Columns() []ColumnType
	Rows() (Rows, error)
	Insert(row []sql.Value) error
}
