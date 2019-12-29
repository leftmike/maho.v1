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

type MakeVirtual func(ctx context.Context, tx Transaction, tn sql.TableName) (Table, error)

type ColumnKey int

func MakeColumnKey(num int, reverse bool) ColumnKey {
	if num < 0 {
		panic("column numbers must be non-negative")
	}
	if reverse {
		return ColumnKey(-num)
	}
	return ColumnKey(num)
}

func (ck ColumnKey) Reverse() bool {
	return ck < 0
}

func (ck ColumnKey) Number() int {
	if ck < 0 {
		return int(-ck)
	}
	return int(ck)
}

type Engine interface {
	CreateSystemTable(tblname sql.Identifier, maker MakeVirtual)
	CreateInfoTable(tblname sql.Identifier, maker MakeVirtual)

	CreateDatabase(dbname sql.Identifier, options Options) error
	DropDatabase(dbname sql.Identifier, ifExists bool, options Options) error

	CreateSchema(ctx context.Context, tx Transaction, sn sql.SchemaName) error
	DropSchema(ctx context.Context, tx Transaction, sn sql.SchemaName, ifExists bool) error

	LookupTable(ctx context.Context, tx Transaction, tn sql.TableName) (Table, error)
	CreateTable(ctx context.Context, tx Transaction, tn sql.TableName, cols []sql.Identifier,
		colTypes []sql.ColumnType, primary []ColumnKey, ifNotExists bool) error
	DropTable(ctx context.Context, tx Transaction, tn sql.TableName, ifExists bool) error

	CreateIndex(ctx context.Context, tx Transaction, idxname sql.Identifier, tn sql.TableName,
		unique bool, keys []ColumnKey, ifNotExists bool) error
	DropIndex(ctx context.Context, tx Transaction, idxname sql.Identifier, tn sql.TableName,
		ifExists bool) error

	Begin(sesid uint64) Transaction
	IsTransactional() bool

	ListDatabases(ctx context.Context, tx Transaction) ([]sql.Identifier, error)
	ListSchemas(ctx context.Context, tx Transaction, dbname sql.Identifier) ([]sql.Identifier,
		error)
	ListTables(ctx context.Context, tx Transaction, sn sql.SchemaName) ([]sql.Identifier, error)
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
	PrimaryKey(ctx context.Context) []ColumnKey
	Seek(ctx context.Context, row []sql.Value) (Rows, error)
	Rows(ctx context.Context) (Rows, error)
	Insert(ctx context.Context, row []sql.Value) error
}
