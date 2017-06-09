package store

import (
	"maho/sql"
)

type Store interface {
	Type() sql.Identifier
	CreateTable(name sql.Identifier, cols []sql.Column) error
	Table(name sql.Identifier) (Table, error)
	Tables() ([]sql.Identifier, [][]sql.Column)
}

type Table interface {
	Name() sql.Identifier
	Columns() []sql.Column
	Rows() (Rows, error)
}

type Rows interface {
	Columns() []sql.Column
	Close() error
	Next(dest []sql.Value) error
}
