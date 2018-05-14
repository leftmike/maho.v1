package db

import (
	"github.com/leftmike/maho/session"
	"github.com/leftmike/maho/sql"
)

type ColumnUpdate struct {
	Index int
	Value sql.Value
}

type Rows interface {
	Columns() []sql.Identifier
	Close() error
	Next(ctx session.Context, dest []sql.Value) error
	Delete(ctx session.Context) error
	Update(ctx session.Context, updates []ColumnUpdate) error
}
