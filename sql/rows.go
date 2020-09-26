package sql

import (
	"context"
)

type Rows interface {
	NumColumns() int
	Close() error
	Next(ctx context.Context, dest []Value) error
	Delete(ctx context.Context) error
	Update(ctx context.Context, updates []ColumnUpdate) error
}

type IndexRows interface {
	Close() error
	Next(ctx context.Context, dest []Value) error
	Delete(ctx context.Context) error
	Update(ctx context.Context, updates []ColumnUpdate) error
	Row(ctx context.Context, dest []Value) error
}
