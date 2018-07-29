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
	ErrorAttaching
	ErrorCreating
	ErrorDetaching
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
	case ErrorAttaching:
		return "error attaching"
	case ErrorCreating:
		return "error creating"
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
}

type databaseEntry struct {
	database Database
	state    databaseState
	name     sql.Identifier
	path     string
	typ      string
	err      error
}

type TID uint64

type Manager struct {
	mutex         sync.RWMutex
	dataDir       string
	engines       map[string]Engine
	databases     map[sql.Identifier]*databaseEntry
	virtualTables TableMap
	lastTID       TID
	lockService   fatlock.Service
}

func NewManager(dataDir string, engines map[string]Engine) *Manager {
	m := Manager{
		dataDir:       dataDir,
		engines:       engines,
		databases:     map[sql.Identifier]*databaseEntry{},
		virtualTables: TableMap{},
	}

	m.lockService.Init()

	m.CreateVirtualTable(sql.ID("db$tables"), m.makeTablesVirtual)
	m.CreateVirtualTable(sql.ID("db$columns"), m.makeColumnsVirtual)
	m.CreateVirtualDatabase(sql.ID("system"), TableMap{
		sql.ID("databases"):   m.makeDatabasesVirtual,
		sql.ID("identifiers"): makeIdentifiersVirtual,
		sql.ID("config"):      makeConfigVirtual,
		sql.ID("engines"):     m.makeEnginesVirtual,
		sql.ID("locks"):       m.makeLocksVirtual,
	})

	return &m
}

func (m *Manager) LockService() fatlock.LockService {
	return &m.lockService
}

func (m *Manager) newDatabaseEntry(eng string, name sql.Identifier, options Options,
	state databaseState) (Engine, *databaseEntry, error) {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.databases[name]; ok {
		return nil, nil, fmt.Errorf("engine: database %s already exists", name)
	}

	typ, ok := options[sql.ENGINE]
	if !ok {
		typ = eng
	} else {
		delete(options, sql.ENGINE)
	}
	e, ok := m.engines[typ]
	if !ok {
		return nil, nil, fmt.Errorf("engine: type %s not found", typ)
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
		typ:   typ,
	}
	m.databases[name] = de
	return e, de, nil
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

func (m *Manager) prepareDatabase(eng string, name sql.Identifier, options Options,
	state databaseState) error {

	e, de, err := m.newDatabaseEntry(eng, name, options, state)
	if err != nil {
		return err
	}

	_, ok := options[sql.WAIT]
	if ok {
		delete(options, sql.WAIT)
		m.setupDatabase(e, de, options)
		return de.err
	}

	go m.setupDatabase(e, de, options)
	return nil
}

func (m *Manager) AttachDatabase(eng string, name sql.Identifier, options Options) error {
	return m.prepareDatabase(eng, name, options, Attaching)
}

func (m *Manager) CreateDatabase(eng string, name sql.Identifier, options Options) error {
	return m.prepareDatabase(eng, name, options, Creating)
}

func (m *Manager) DetachDatabase(name sql.Identifier) error {
	return nil // XXX
}

type Transaction struct {
	lockerState fatlock.LockerState
	lockService *fatlock.Service
	contexts    map[Database]interface{}
	tid         TID
	name        string
}

// Begin a new transaction.
func (m *Manager) Begin() *Transaction {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.lastTID += 1
	return &Transaction{
		lockService: &m.lockService,
		contexts:    map[Database]interface{}{},
		tid:         m.lastTID,
		name:        fmt.Sprintf("transaction-%d", m.lastTID),
	}
}

func (tx *Transaction) LockerState() *fatlock.LockerState {
	return &tx.lockerState
}

func (tx *Transaction) String() string {
	return tx.name
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
	return err
}

func (tx *Transaction) Rollback() error {
	err := tx.forContexts(func(d Database, tctx interface{}) error {
		return d.Rollback(tctx)
	})
	tx.contexts = nil
	tx.lockService.ReleaseLocks(tx)
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
