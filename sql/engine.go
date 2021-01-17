package sql

import (
	"context"

	"github.com/leftmike/maho/flags"
)

type RefAction int

const (
	NoAction RefAction = iota
	Restrict
	Cascade
	SetNull
	SetDefault
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
		colDefaults []ColumnDefault, cons []Constraint, ifNotExists bool) error
	DropTable(ctx context.Context, tn TableName, ifExists, cascade bool) error
	AddForeignKey(ctx context.Context, con Identifier, fktn TableName, fkCols []int, rtn TableName,
		ridx Identifier, onDel, onUpd RefAction, check bool) error
	AddTrigger(ctx context.Context, tn TableName, events int64, trig Trigger) error
	DropConstraint(ctx context.Context, tn TableName, con Identifier, ifExists bool,
		col Identifier, ct ConstraintType) error

	CreateIndex(ctx context.Context, idxname Identifier, tn TableName, unique bool,
		keys []ColumnKey, ifNotExists bool) error
	DropIndex(ctx context.Context, idxname Identifier, tn TableName, ifExists bool) error
}

type IndexType struct {
	Name    Identifier
	Key     []ColumnKey
	Columns []int
	Unique  bool
	Hidden  bool
}

type TableType interface {
	Version() int64
	Columns() []Identifier
	ColumnTypes() []ColumnType
	ColumnDefaults() []ColumnDefault
	PrimaryKey() []ColumnKey
	Indexes() []IndexType
}

const (
	DeleteEvent = 1 << iota
	InsertEvent
	UpdateEvent
)

type Table interface {
	Rows(ctx context.Context, minRow, maxRow []Value) (Rows, error)
	IndexRows(ctx context.Context, iidx int, minRow, maxRow []Value) (IndexRows, error)
	Insert(ctx context.Context, rows [][]Value) error
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

	ValidDatabase(dbname Identifier) (bool, error)
	CreateDatabase(dbname Identifier, options map[Identifier]string) error
	DropDatabase(dbname Identifier, ifExists bool, options map[Identifier]string) error

	Begin(sesid uint64) Transaction
}
