package db

import (
	"github.com/leftmike/maho/sql"
)

type Rows interface {
	Columns() []sql.Identifier
	Close() error
	Next(dest []sql.Value) error
}

type DeleteRows interface {
	Rows
	Delete() error
}

type ColumnUpdate struct {
	Index int
	Value sql.Value
}

type UpdateRows interface {
	Rows
	Update(updates []ColumnUpdate) error
}
