package sql

import (
	"context"
)

type Transaction interface {
	Commit(ctx context.Context) error
	Rollback() error
	NextStmt(ctx context.Context) error
}

type IndexType struct {
	Name    Identifier
	Key     []ColumnKey
	Columns []int
	Unique  bool
}

type TableType interface {
	Columns() []Identifier
	ColumnTypes() []ColumnType
	PrimaryKey() []ColumnKey
	Indexes() []IndexType
}

type Table interface {
	Rows(ctx context.Context, minRow, maxRow []Value) (Rows, error)
	Insert(ctx context.Context, row []Value) error
}

type MakeVirtual func(ctx context.Context, tx Transaction, tn TableName) (Table, TableType, error)

type Engine interface {
	CreateSystemInfoTable(tblname Identifier, maker MakeVirtual)
	CreateMetadataTable(tblname Identifier, maker MakeVirtual)

	CreateDatabase(dbname Identifier, options map[Identifier]string) error
	DropDatabase(dbname Identifier, ifExists bool, options map[Identifier]string) error

	CreateSchema(ctx context.Context, tx Transaction, sn SchemaName) error
	DropSchema(ctx context.Context, tx Transaction, sn SchemaName, ifExists bool) error

	LookupTableType(ctx context.Context, tx Transaction, tn TableName) (TableType, error)
	LookupTable(ctx context.Context, tx Transaction, tn TableName) (Table, TableType, error)
	CreateTable(ctx context.Context, tx Transaction, tn TableName, cols []Identifier,
		colTypes []ColumnType, cons []Constraint, ifNotExists bool) error
	DropTable(ctx context.Context, tx Transaction, tn TableName, ifExists bool) error
	AddForeignKey(ctx context.Context, tx Transaction, con Identifier, fktn TableName,
		fkCols []int, rtn TableName, ridx Identifier) error

	CreateIndex(ctx context.Context, tx Transaction, idxname Identifier, tn TableName, unique bool,
		keys []ColumnKey, ifNotExists bool) error
	DropIndex(ctx context.Context, tx Transaction, idxname Identifier, tn TableName,
		ifExists bool) error

	Begin(sesid uint64) Transaction
	ListDatabases(ctx context.Context, tx Transaction) ([]Identifier, error)
}