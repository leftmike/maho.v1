package db

import (
	"github.com/leftmike/maho/sql"
)

type Table interface {
	Columns() []sql.Identifier
	ColumnTypes() []ColumnType
	Rows() (Rows, error)
	Insert(row []sql.Value) error
}
