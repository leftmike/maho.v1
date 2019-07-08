package engine

import (
	"context"

	"github.com/leftmike/maho/sql"
)

type Options map[sql.Identifier]string

type Transaction interface {
	Commit(ctx context.Context) error
	Rollback() error
	NextStmt()
}

type MakeVirtual func(ctx context.Context, tx Transaction, dbname, tblname sql.Identifier) (Table,
	error)

type Engine interface {
	CreateSystemTable(tblname sql.Identifier, maker MakeVirtual)
	CreateInfoTable(tblname sql.Identifier, maker MakeVirtual)
	AttachDatabase(name sql.Identifier, options Options) error
	CreateDatabase(name sql.Identifier, options Options) error
	DetachDatabase(name sql.Identifier, options Options) error
	DropDatabase(name sql.Identifier, exists bool, options Options) error
	LookupTable(ctx context.Context, tx Transaction, dbname, tblname sql.Identifier) (Table, error)
	CreateTable(ctx context.Context, tx Transaction, dbname, tblname sql.Identifier,
		cols []sql.Identifier, colTypes []sql.ColumnType) error
	DropTable(ctx context.Context, tx Transaction, dbname, tblname sql.Identifier,
		exists bool) error
	Begin(sid uint64) Transaction
}

type Rows interface {
	Columns() []sql.Identifier
	Close() error
	Next(ctx context.Context, dest []sql.Value) error
	Delete(ctx context.Context) error
	Update(ctx context.Context, updates []sql.ColumnUpdate) error
}

type Table interface {
	Columns(ctx context.Context) []sql.Identifier
	ColumnTypes(ctx context.Context) []sql.ColumnType
	Rows(ctx context.Context) (Rows, error)
	Insert(ctx context.Context, row []sql.Value) error
}
