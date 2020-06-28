package storage

import (
	"context"

	"github.com/leftmike/maho/sql"
)

// TableStructure --> tblstore.TableStructure: TableDef to TableStructure
// TableMetadata --> save for encoded layout
// TableDefinition --> interface defined here

type TableDefinition interface {
	Columns() []sql.Identifier
	ColumnTypes() []sql.ColumnType
	PrimaryKey() []sql.ColumnKey
}

type Engine interface {
	EncodeTableDefinition(td TableDefinition) ([]byte, error)
	DecodeTableDefinition(buf []byte) (TableDefinition, error)
	MakeTableDefinition(cols []sql.Identifier, colTypes []sql.ColumnType,
		primary []sql.ColumnKey) (TableDefinition, error)
}

type Store interface {
	//SetEngine(e Engine)
	CreateDatabase(dbname sql.Identifier, options map[sql.Identifier]string) error
	DropDatabase(dbname sql.Identifier, ifExists bool, options map[sql.Identifier]string) error

	CreateSchema(ctx context.Context, tx Transaction, sn sql.SchemaName) error
	DropSchema(ctx context.Context, tx Transaction, sn sql.SchemaName, ifExists bool) error

	LookupTable(ctx context.Context, tx Transaction, tn sql.TableName) (Table, error)
	CreateTable(ctx context.Context, tx Transaction, tn sql.TableName, cols []sql.Identifier,
		colTypes []sql.ColumnType, primary []sql.ColumnKey, ifNotExists bool) error
	DropTable(ctx context.Context, tx Transaction, tn sql.TableName, ifExists bool) error

	CreateIndex(ctx context.Context, tx Transaction, idxname sql.Identifier, tn sql.TableName,
		unique bool, keys []sql.ColumnKey, ifNotExists bool) error
	DropIndex(ctx context.Context, tx Transaction, idxname sql.Identifier, tn sql.TableName,
		ifExists bool) error

	Begin(sesid uint64) Transaction

	ListDatabases(ctx context.Context, tx Transaction) ([]sql.Identifier, error)
	ListSchemas(ctx context.Context, tx Transaction, dbname sql.Identifier) ([]sql.Identifier,
		error)
	ListTables(ctx context.Context, tx Transaction, sn sql.SchemaName) ([]sql.Identifier, error)
}

type Transaction interface {
	Commit(ctx context.Context) error
	Rollback() error
	NextStmt()
}

type Table interface {
	Columns(ctx context.Context) []sql.Identifier
	ColumnTypes(ctx context.Context) []sql.ColumnType
	PrimaryKey(ctx context.Context) []sql.ColumnKey
	Rows(ctx context.Context, minRow, maxRow []sql.Value) (sql.Rows, error)
	Insert(ctx context.Context, row []sql.Value) error
}
