package db

import (
	"github.com/leftmike/maho/sql"
)

type Table interface {
	Name() sql.Identifier
	Columns() []sql.Identifier
	ColumnTypes() []ColumnType
	Rows() (Rows, error)
}

type TableModify interface {
	Table
	Insert(row []sql.Value) error
	DeleteRows() (DeleteRows, error)
	UpdateRows() (UpdateRows, error)
}
