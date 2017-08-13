package db

import (
	"maho/sql"
)

type Table interface {
	Name() sql.Identifier
	Columns() []sql.Identifier
	ColumnTypes() []ColumnType
	Rows() (Rows, error)
	Insert(row []sql.Value) error
}
