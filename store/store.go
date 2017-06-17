package store

import (
	"maho/sql"
)

type Store interface {
	Name() sql.Identifier
	Type() sql.Identifier
	CreateTable(name sql.Identifier, cols []sql.Column) error
	Table(name sql.Identifier) (Table, error)
	Tables() ([]sql.Identifier, [][]sql.Column)
}

type ColumnMap map[sql.Identifier]int

type Table interface {
	Name() sql.Identifier
	Columns() []sql.Column
	ColumnMap() ColumnMap
	Rows() (Rows, error)
	Insert(row []sql.Value) error
}

type Rows interface {
	Columns() []sql.Column
	Close() error
	Next(dest []sql.Value) error
}
