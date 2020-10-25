package sql

import (
	"context"

	"github.com/leftmike/maho/flags"
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
	AddTrigger(ctx context.Context, tn TableName, events int64, trig Trigger) error

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

const (
	DeleteEvent = 1 << iota
	InsertEvent
	UpdateEvent
)

type Table interface {
	ModifyStart(ctx context.Context, event int64) error
	ModifyDone(ctx context.Context, event, cnt int64) (int64, error)
	Rows(ctx context.Context, minRow, maxRow []Value) (Rows, error)
	IndexRows(ctx context.Context, iidx int, minRow, maxRow []Value) (IndexRows, error)
	Insert(ctx context.Context, row []Value) error
}

type Trigger interface {
	Type() string
	Encode() ([]byte, error)
	AfterRows(ctx context.Context, tx Transaction, tbl Table, oldRows, newRows Rows) error
}

type MakeVirtual func(ctx context.Context, tx Transaction, tn TableName) (Table, TableType, error)

type Engine interface {
	GetFlag(f flags.Flag) bool

	CreateSystemInfoTable(tblname Identifier, maker MakeVirtual)
	CreateMetadataTable(tblname Identifier, maker MakeVirtual)

	CreateDatabase(dbname Identifier, options map[Identifier]string) error
	DropDatabase(dbname Identifier, ifExists bool, options map[Identifier]string) error

	Begin(sesid uint64) Transaction
}
