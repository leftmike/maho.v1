package db

import (
	"maho/sql"
)

type Table interface {
	Name() sql.Identifier
	Columns() []sql.Identifier
	ColumnTypes() []ColumnType
	Rows() (Rows, error)
}

type TableInsert interface {
	Table
	Insert(row []sql.Value) error
}
