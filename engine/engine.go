package engine

import (
	"context"
	"fmt"
	"sync"

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

type TransactionState struct {
	tid uint64
	sid uint64
}

type Engine interface {
	AttachDatabase(name sql.Identifier, options Options) (Database, error)
	CreateDatabase(name sql.Identifier, options Options) (Database, error)
	Begin(sid uint64) Transaction
	//Locks() []service.Lock XXX
	Transactions() []TransactionState
}

type databaseState int

const (
	Attaching databaseState = iota
	Creating
	Detaching
	Dropping
	ErrorAttaching
	ErrorCreating
	ErrorDetaching
	ErrorDropping
	Running
)

func (ds databaseState) String() string {
	switch ds {
	case Attaching:
		return "attaching"
	case Creating:
		return "creating"
	case Detaching:
		return "detaching"
	case Dropping:
		return "dropping"
	case ErrorAttaching:
		return "error attaching"
	case ErrorCreating:
		return "error creating"
	case ErrorDetaching:
		return "error detaching"
	case ErrorDropping:
		return "error dropping"
	case Running:
		return "running"
	default:
		panic(fmt.Sprintf("unexpected value for database state: %d", ds))
	}
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

type Database interface {
	Message() string
	LookupTable(ses Session, tx Transaction, tblname sql.Identifier) (Table, error)
	CreateTable(ses Session, tx Transaction, tblname sql.Identifier, cols []sql.Identifier,
		colTypes []sql.ColumnType) error
	DropTable(ses Session, tx Transaction, tblname sql.Identifier, exists bool) error
	ListTables(ses Session, tx Transaction) ([]TableEntry, error)
	CanClose(drop bool) bool
	Close(drop bool) error
}

type databaseEntry struct {
	database Database
	state    databaseState
	name     sql.Identifier
	typ      string
	err      error
}

type Manager struct {
	mutex         sync.RWMutex
	dataDir       string
	engine        Engine
	databases     map[sql.Identifier]*databaseEntry
	virtualTables TableMap
	systemTables  TableMap
}

func NewManager(dataDir string, eng Engine) *Manager {
	m := Manager{
		dataDir:       dataDir,
		engine:        eng,
		databases:     map[sql.Identifier]*databaseEntry{},
		virtualTables: TableMap{},
	}
	m.systemTables = TableMap{
		//sql.ID("databases"):   m.makeDatabasesVirtual,
		sql.ID("identifiers"): makeIdentifiersVirtual,
		sql.ID("config"):      makeConfigVirtual,
		//sql.ID("locks"):        m.makeLocksVirtual,
		sql.ID("transactions"): m.makeTransactionsVirtual,
	}

	m.CreateVirtualTable(sql.ID("db$tables"), m.makeTablesVirtual)
	m.CreateVirtualTable(sql.ID("db$columns"), m.makeColumnsVirtual)
	m.CreateVirtualDatabase(sql.ID("system"), m.systemTables)

	return &m
}

func (m *Manager) canSetupDatabase(name sql.Identifier, options Options,
	state databaseState) (Engine, *databaseEntry, error) {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.databases[name]; ok {
		return nil, nil, fmt.Errorf("engine: database %s already exists", name)
	}

	de := &databaseEntry{
		state: state,
		name:  name,
	}
	m.databases[name] = de
	return m.engine, de, nil
}

func (m *Manager) setupDatabase(e Engine, de *databaseEntry, options Options) {
	var d Database
	if de.state == Attaching {
		d, de.err = e.AttachDatabase(de.name, options)
	} else {
		// de.state == Creating
		d, de.err = e.CreateDatabase(de.name, options)
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if de.err == nil {
		de.state = Running
		de.database = d
	} else {
		if de.state == Attaching {
			de.state = ErrorAttaching
		} else {
			de.state = ErrorCreating
		}
	}
}

func (m *Manager) trySetupDatabase(name sql.Identifier, options Options,
	state databaseState) error {

	e, de, err := m.canSetupDatabase(name, options, state)
	if err != nil {
		return err
	}

	m.setupDatabase(e, de, options)
	return de.err
}

func (m *Manager) AttachDatabase(name sql.Identifier, options Options) error {
	return m.trySetupDatabase(name, options, Attaching)
}

func (m *Manager) CreateDatabase(name sql.Identifier, options Options) error {
	return m.trySetupDatabase(name, options, Creating)
}

func (m *Manager) canCloseDatabase(name sql.Identifier, exists bool, options Options,
	state databaseState) (*databaseEntry, error) {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	de, ok := m.databases[name]
	if !ok {
		if exists {
			return nil, nil
		}
		return nil, fmt.Errorf("engine: database %s not found", name)
	}

	if de.state == Attaching || de.state == Creating || de.state == Detaching ||
		de.state == Dropping {

		return nil, fmt.Errorf(
			"engine: database %s is already attaching, creating, detaching, or dropping", name)
	}

	if de.state == Running {
		if !de.database.CanClose(state == Dropping) {
			return nil, fmt.Errorf("engine: database %s can not be closed", name)
		}
		de.state = state
		return de, nil
	} else {
		delete(m.databases, name)
	}

	return nil, nil
}

func (m *Manager) closeDatabase(name sql.Identifier, de *databaseEntry) {
	de.err = de.database.Close(de.state == Dropping)

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if de.err == nil {
		delete(m.databases, name)
	} else {
		if de.state == Detaching {
			de.state = ErrorDetaching
		} else {
			de.state = ErrorDropping
		}
	}
}

func (m *Manager) tryCloseDatabase(name sql.Identifier, exists bool, options Options,
	state databaseState) error {

	de, err := m.canCloseDatabase(name, exists, options, state)
	if de == nil || err != nil {
		return err
	}

	m.closeDatabase(name, de)
	return de.err
}

func (m *Manager) DetachDatabase(name sql.Identifier, options Options) error {
	return m.tryCloseDatabase(name, false, options, Detaching)
}

func (m *Manager) DropDatabase(name sql.Identifier, exists bool, options Options) error {
	return m.tryCloseDatabase(name, exists, options, Dropping)
}

func (m *Manager) Begin(sid uint64) Transaction {
	return m.engine.Begin(sid)
}

// LookupTable looks up the named table in the named database.
func (m *Manager) LookupTable(ses Session, tx Transaction, dbname, tblname sql.Identifier) (Table,
	error) {

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	de, ok := m.databases[dbname]
	if !ok {
		return nil, fmt.Errorf("engine: database %s not found", dbname)
	}
	if de.state != Running {
		return nil, fmt.Errorf("engine: database %s not running", dbname)
	}
	tbl, err := m.lookupVirtual(ses, tx, de.database, dbname, tblname)
	if tbl != nil || err != nil {
		return tbl, err
	}
	return de.database.LookupTable(ses, tx, tblname)
}

// CreateTable creates the named table in the named database.
func (m *Manager) CreateTable(ses Session, tx Transaction, dbname, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []sql.ColumnType) error {

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	de, ok := m.databases[dbname]
	if !ok {
		return fmt.Errorf("engine: database %s not found", dbname)
	}
	if de.state != Running {
		return fmt.Errorf("engine: database %s not running", dbname)
	}
	return de.database.CreateTable(ses, tx, tblname, cols, colTypes)
}

// DropTable drops the named table from the named database.
func (m *Manager) DropTable(ses Session, tx Transaction, dbname, tblname sql.Identifier,
	exists bool) error {

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	de, ok := m.databases[dbname]
	if !ok {
		return fmt.Errorf("engine: database %s not found", dbname)
	}
	if de.state != Running {
		return fmt.Errorf("engine: database %s not running", dbname)
	}
	return de.database.DropTable(ses, tx, tblname, exists)
}
