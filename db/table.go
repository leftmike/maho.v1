package db

import (
	"github.com/leftmike/maho/sql"
)

type Table interface {
	Name() sql.Identifier
	Columns() []sql.Identifier
	ColumnTypes() []ColumnType
	Rows() (Rows, error)
	DeleteRows() (DeleteRows, error)
}

type TableInsert interface {
	Table
	Insert(row []sql.Value) error
}
