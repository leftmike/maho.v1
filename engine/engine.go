package engine

import (
	"context"

	"github.com/leftmike/maho/sql"
)

type TableType int

const (
	PhysicalType TableType = iota
	VirtualType
)

type TableEntry struct {
	Name sql.Identifier
	Type TableType
}

type Options map[sql.Identifier]string

type Transaction interface {
	Commit(ses Session) error
	Rollback() error
	NextStmt()
}

type MakeVirtual func(ses Session, tx Transaction, d Database, dbname,
	tblname sql.Identifier) (Table, error)

type Engine interface {
	CreateSystemTable(tblname sql.Identifier, maker MakeVirtual)
	CreateInfoTable(tblname sql.Identifier, maker MakeVirtual)
	AttachDatabase(name sql.Identifier, options Options) error
	CreateDatabase(name sql.Identifier, options Options) error
	DetachDatabase(name sql.Identifier, options Options) error
	DropDatabase(name sql.Identifier, exists bool, options Options) error
	LookupTable(ses Session, tx Transaction, dbname, tblname sql.Identifier) (Table, error)
	CreateTable(ses Session, tx Transaction, dbname, tblname sql.Identifier,
		cols []sql.Identifier, colTypes []sql.ColumnType) error
	DropTable(ses Session, tx Transaction, dbname, tblname sql.Identifier,
		exists bool) error
	Begin(sid uint64) Transaction
}

type Session interface {
	Context() context.Context
}

type Rows interface {
	Columns() []sql.Identifier
	Close() error
	Next(ses Session, dest []sql.Value) error
	Delete(ses Session) error
	Update(ses Session, updates []sql.ColumnUpdate) error
}

type Table interface {
	Columns(ses Session) []sql.Identifier
	ColumnTypes(ses Session) []sql.ColumnType
	Rows(ses Session) (Rows, error)
	Insert(ses Session, row []sql.Value) error
}

type Database interface{}

/* XXX
	Message() string
	LookupTable(ses Session, tx Transaction, tblname sql.Identifier) (Table, error)
	CreateTable(ses Session, tx Transaction, tblname sql.Identifier, cols []sql.Identifier,
		colTypes []sql.ColumnType) error
	DropTable(ses Session, tx Transaction, tblname sql.Identifier, exists bool) error
	ListTables(ses Session, tx Transaction) ([]TableEntry, error)
	CanClose(drop bool) bool
	Close(drop bool) error
}*/

type Manager struct {
	eng Engine
}

func NewManager(dataDir string, eng Engine) *Manager {
	return &Manager{
		eng: eng,
	}
}

func (m *Manager) CreateSystemTable(tblname sql.Identifier, maker MakeVirtual) {
	m.eng.CreateSystemTable(tblname, maker)
}

func (m *Manager) AttachDatabase(name sql.Identifier, options Options) error {
	return m.eng.AttachDatabase(name, options)
}

func (m *Manager) CreateDatabase(name sql.Identifier, options Options) error {
	return m.eng.CreateDatabase(name, options)
}

func (m *Manager) DetachDatabase(name sql.Identifier, options Options) error {
	return m.eng.DetachDatabase(name, options)
}

func (m *Manager) DropDatabase(name sql.Identifier, exists bool, options Options) error {
	return m.eng.DropDatabase(name, exists, options)
}

func (m *Manager) Begin(sid uint64) Transaction {
	return m.eng.Begin(sid)
}

func (m *Manager) LookupTable(ses Session, tx Transaction, dbname, tblname sql.Identifier) (Table,
	error) {

	return m.eng.LookupTable(ses, tx, dbname, tblname)
}

func (m *Manager) CreateTable(ses Session, tx Transaction, dbname, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []sql.ColumnType) error {

	return m.eng.CreateTable(ses, tx, dbname, tblname, cols, colTypes)
}

// DropTable drops the named table from the named database.
func (m *Manager) DropTable(ses Session, tx Transaction, dbname, tblname sql.Identifier,
	exists bool) error {

	return m.eng.DropTable(ses, tx, dbname, tblname, exists)
}
