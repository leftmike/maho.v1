package db

import (
	"maho/sql"
)

type ColumnMap map[sql.Identifier]int

type Table interface {
	Name() sql.Identifier
	Columns() []ColumnType
	ColumnMap() ColumnMap
	Rows() (Rows, error)
	Insert(row []sql.Value) error
}
