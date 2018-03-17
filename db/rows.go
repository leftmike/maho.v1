package db

import (
	"context"

	"github.com/leftmike/maho/sql"
)

type ColumnUpdate struct {
	Index int
	Value sql.Value
}

type Rows interface {
	Columns() []sql.Identifier
	Close() error
	Next(ctx context.Context, dest []sql.Value) error
	Delete(ctx context.Context) error
	Update(ctx context.Context, updates []ColumnUpdate) error
}
