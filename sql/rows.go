package sql

import (
	"context"
)

type Rows interface {
	Columns() []Identifier
	Close() error
	Next(ctx context.Context, dest []Value) error
	Delete(ctx context.Context) error
	Update(ctx context.Context, updates []ColumnUpdate) error
}
