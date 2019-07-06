package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/leftmike/maho/engine/fatlock"
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

type Services interface {
	LockService() fatlock.LockService
}

type Options map[sql.Identifier]string

type Engine interface {
	AttachDatabase(svcs Services, name sql.Identifier, path string, options Options) (Database,
		error)
	CreateDatabase(svcs Services, name sql.Identifier, path string, options Options) (Database,
		error)
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
	LookupTable(ses Session, tctx interface{}, tblname sql.Identifier) (Table, error)
	CreateTable(ses Session, tctx interface{}, tblname sql.Identifier, cols []sql.Identifier,
		colTypes []sql.ColumnType) error
	DropTable(ses Session, tctx interface{}, tblname sql.Identifier, exists bool) error
	ListTables(ses Session, tctx interface{}) ([]TableEntry, error)
	Begin(lkr fatlock.Locker) interface{}
	Commit(ses Session, tctx interface{}) error
	Rollback(tctx interface{}) error
	NextStmt(tctx interface{})
	CanClose(drop bool) bool
	Close(drop bool) error
}

type databaseEntry struct {
	database Database
	state    databaseState
	name     sql.Identifier
	path     string
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
	lastTID       uint64
	lockService   fatlock.Service
	transactions  map[*Transaction]struct{}
}

func NewManager(dataDir string, eng Engine) *Manager {
	m := Manager{
		dataDir:       dataDir,
		engine:        eng,
		databases:     map[sql.Identifier]*databaseEntry{},
		virtualTables: TableMap{},
		transactions:  map[*Transaction]struct{}{},
	}
	m.systemTables = TableMap{
		sql.ID("databases"):    m.makeDatabasesVirtual,
		sql.ID("identifiers"):  makeIdentifiersVirtual,
		sql.ID("config"):       makeConfigVirtual,
		sql.ID("locks"):        m.makeLocksVirtual,
		sql.ID("transactions"): m.makeTransactionsVirtual,
	}

	m.lockService.Init()

	m.CreateVirtualTable(sql.ID("db$tables"), m.makeTablesVirtual)
	m.CreateVirtualTable(sql.ID("db$columns"), m.makeColumnsVirtual)
	m.CreateVirtualDatabase(sql.ID("system"), m.systemTables)

	return &m
}

func (m *Manager) LockService() fatlock.LockService {
	return &m.lockService
}

func (m *Manager) canSetupDatabase(name sql.Identifier, options Options,
	state databaseState) (Engine, *databaseEntry, error) {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.databases[name]; ok {
		return nil, nil, fmt.Errorf("engine: database %s already exists", name)
	}

	path, ok := options[sql.PATH]
	if !ok {
		path = filepath.Join(m.dataDir, name.String())
	} else {
		delete(options, sql.PATH)
	}
	de := &databaseEntry{
		state: state,
		name:  name,
		path:  path,
	}
	m.databases[name] = de
	return m.engine, de, nil
}

func (m *Manager) setupDatabase(e Engine, de *databaseEntry, options Options) {
	var d Database
	if de.state == Attaching {
		d, de.err = e.AttachDatabase(m, de.name, de.path, options)
	} else {
		// de.state == Creating
		d, de.err = e.CreateDatabase(m, de.name, de.path, options)
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

type Transaction struct {
	m           *Manager
	lockerState fatlock.LockerState
	lockService *fatlock.Service
	contexts    map[Database]interface{}
	tid         uint64
	sid         uint64
}

func (m *Manager) removeTransaction(tx *Transaction) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.transactions, tx)
}

// Begin a new transaction.
func (m *Manager) Begin(sid uint64) *Transaction {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.lastTID += 1
	tx := &Transaction{
		m:           m,
		lockService: &m.lockService,
		contexts:    map[Database]interface{}{},
		tid:         m.lastTID,
		sid:         sid,
	}
	m.transactions[tx] = struct{}{}
	return tx
}

func (tx *Transaction) LockerState() *fatlock.LockerState {
	return &tx.lockerState
}

func (tx *Transaction) String() string {
	return fmt.Sprintf("transaction-%d", tx.tid)
}

func (tx *Transaction) forContexts(fn func(d Database, tctx interface{}) error) error {
	if tx.contexts == nil {
		panic("transaction used after commit or rollback")
	}

	var err error
	for d, tctx := range tx.contexts {
		cerr := fn(d, tctx)
		if cerr != nil {
			if err == nil {
				err = cerr
			} else {
				err = fmt.Errorf("%s; %s", err, cerr)
			}
		}
	}
	return err
}

func (tx *Transaction) Commit(ses Session) error {
	err := tx.forContexts(func(d Database, tctx interface{}) error {
		return d.Commit(ses, tctx)
	})
	tx.contexts = nil
	tx.lockService.ReleaseLocks(tx)
	tx.m.removeTransaction(tx)
	return err
}

func (tx *Transaction) Rollback() error {
	err := tx.forContexts(func(d Database, tctx interface{}) error {
		return d.Rollback(tctx)
	})
	tx.contexts = nil
	tx.lockService.ReleaseLocks(tx)
	tx.m.removeTransaction(tx)
	return err
}

func (tx *Transaction) NextStmt() {
	tx.forContexts(func(d Database, tctx interface{}) error {
		d.NextStmt(tctx)
		return nil
	})
}

func (tx *Transaction) getContext(d Database) interface{} {
	if tx.contexts == nil {
		panic("transaction used after commit or rollback")
	}

	tctx, ok := tx.contexts[d]
	if !ok {
		tctx = d.Begin(tx)
		tx.contexts[d] = tctx
	}
	return tctx
}

// LookupTable looks up the named table in the named database.
func (m *Manager) LookupTable(ses Session, tx *Transaction, dbname, tblname sql.Identifier) (Table,
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
	tctx := tx.getContext(de.database)
	tbl, err := m.lookupVirtual(ses, tctx, de.database, dbname, tblname)
	if tbl != nil || err != nil {
		return tbl, err
	}
	return de.database.LookupTable(ses, tctx, tblname)
}

// CreateTable creates the named table in the named database.
func (m *Manager) CreateTable(ses Session, tx *Transaction, dbname, tblname sql.Identifier,
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
	return de.database.CreateTable(ses, tx.getContext(de.database), tblname, cols, colTypes)
}

// DropTable drops the named table from the named database.
func (m *Manager) DropTable(ses Session, tx *Transaction, dbname, tblname sql.Identifier,
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
	return de.database.DropTable(ses, tx.getContext(de.database), tblname, exists)
}
