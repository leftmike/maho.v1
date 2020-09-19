package sql

import (
	"context"
)

type Transaction interface {
	Commit(ctx context.Context) error
	Rollback() error
	NextStmt(ctx context.Context) error

	CreateSchema(ctx context.Context, sn SchemaName) error
	DropSchema(ctx context.Context, sn SchemaName, ifExists bool) error

	LookupTableType(ctx context.Context, tn TableName) (TableType, error)
	LookupTable(ctx context.Context, tn TableName, ttVer int64) (Table, error)
	CreateTable(ctx context.Context, tn TableName, cols []Identifier, colTypes []ColumnType,
		cons []Constraint, ifNotExists bool) error
	DropTable(ctx context.Context, tn TableName, ifExists bool) error
	AddForeignKey(ctx context.Context, con Identifier, fktn TableName, fkCols []int, rtn TableName,
		ridx Identifier) error

	CreateIndex(ctx context.Context, idxname Identifier, tn TableName, unique bool,
		keys []ColumnKey, ifNotExists bool) error
	DropIndex(ctx context.Context, idxname Identifier, tn TableName, ifExists bool) error

	ListDatabases(ctx context.Context) ([]Identifier, error)
}

type IndexType struct {
	Name    Identifier
	Key     []ColumnKey
	Columns []int
	Unique  bool
}

type TableType interface {
	Version() int64
	Columns() []Identifier
	ColumnTypes() []ColumnType
	PrimaryKey() []ColumnKey
	Indexes() []IndexType
}

type Table interface {
	Rows(ctx context.Context, minRow, maxRow []Value) (Rows, error)
	IndexRows(ctx context.Context, iidx int, minRow, maxRow []Value) (IndexRows, error)
	Insert(ctx context.Context, row []Value) error
}

type MakeVirtual func(ctx context.Context, tx Transaction, tn TableName) (Table, TableType, error)

type Engine interface {
	CreateSystemInfoTable(tblname Identifier, maker MakeVirtual)
	CreateMetadataTable(tblname Identifier, maker MakeVirtual)

	CreateDatabase(dbname Identifier, options map[Identifier]string) error
	DropDatabase(dbname Identifier, ifExists bool, options map[Identifier]string) error

	Begin(sesid uint64) Transaction
}
