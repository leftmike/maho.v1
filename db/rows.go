package db

import (
	"context"

	"github.com/leftmike/maho/sql"
)

type Session interface {
	Context() context.Context
	DefaultEngine() string
	DefaultDatabase() sql.Identifier
}

type ColumnUpdate struct {
	Index int
	Value sql.Value
}

type Rows interface {
	Columns() []sql.Identifier
	Close() error
	Next(ses Session, dest []sql.Value) error
	Delete(ses Session) error
	Update(ses Session, updates []ColumnUpdate) error
}
