package engine

import (
	"context"

	"github.com/leftmike/maho/sql"
)

type Transaction interface {
	Commit(ctx context.Context) error
	Rollback() error
	NextStmt()
}

type Table interface {
	Rows(ctx context.Context, minRow, maxRow []sql.Value) (Rows, error)
	IndexRows(ctx context.Context, iidx int, minRow, maxRow []sql.Value) (IndexRows, error)
	Insert(ctx context.Context, row []sql.Value) error
}

type Rows interface {
	NumColumns() int
	Close() error
	Next(ctx context.Context) ([]sql.Value, error)
	Delete(ctx context.Context) error
	Update(ctx context.Context, updatedCols []int, updateRow []sql.Value) error
}

type IndexRows interface {
	Close() error
	Next(ctx context.Context) ([]sql.Value, error)
	Delete(ctx context.Context) error
	Update(ctx context.Context, updatedCols []int, updateRow []sql.Value) error
	Row(ctx context.Context) ([]sql.Value, error)
}

type store interface {
	CreateDatabase(dbname sql.Identifier, options map[sql.Identifier]string) error
	DropDatabase(dbname sql.Identifier, ifExists bool, options map[sql.Identifier]string) error

	CreateSchema(ctx context.Context, tx Transaction, sn sql.SchemaName) error
	DropSchema(ctx context.Context, tx Transaction, sn sql.SchemaName, ifExists bool) error

	LookupTableType(ctx context.Context, tx Transaction, tn sql.TableName) (*TableType, error)
	LookupTable(ctx context.Context, tx Transaction, tn sql.TableName) (Table, *TableType,
		error)
	CreateTable(ctx context.Context, tx Transaction, tn sql.TableName, tt *TableType,
		ifNotExists bool) error
	DropTable(ctx context.Context, tx Transaction, tn sql.TableName, ifExists bool) error
	UpdateType(ctx context.Context, tx Transaction, tn sql.TableName, tt *TableType) error

	AddIndex(ctx context.Context, tx Transaction, tn sql.TableName, tt *TableType,
		it sql.IndexType) error
	RemoveIndex(ctx context.Context, tx Transaction, tn sql.TableName, tt *TableType,
		rdx int) error

	ListDatabases(ctx context.Context, tx Transaction) ([]sql.Identifier, error)
	ListSchemas(ctx context.Context, tx Transaction, dbname sql.Identifier) ([]sql.Identifier,
		error)
	ListTables(ctx context.Context, tx Transaction, sn sql.SchemaName) ([]sql.Identifier,
		error)

	Begin(sesid uint64) Transaction
}
