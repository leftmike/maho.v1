package db

import (
	"github.com/leftmike/maho/sql"
)

type ColumnUpdate struct {
	Index int
	Value sql.Value
}

type Rows interface {
	Columns() []sql.Identifier
	Close() error
	Next(dest []sql.Value) error
	Delete() error
	Update(updates []ColumnUpdate) error
}
