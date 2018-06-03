package engine

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/db"
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

type Engine interface {
	AttachDatabase(name sql.Identifier, path string, options Options) (Database, error)
	CreateDatabase(name sql.Identifier, path string, options Options) (Database, error)
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

type Database interface {
	Message() string
	LookupTable(ses db.Session, tctx interface{}, tblname sql.Identifier) (db.Table, error)
	CreateTable(ses db.Session, tctx interface{}, tblname sql.Identifier,
		cols []sql.Identifier, colTypes []db.ColumnType) error
	DropTable(ses db.Session, tctx interface{}, tblname sql.Identifier, exists bool) error
	ListTables(ses db.Session, tctx interface{}) ([]TableEntry, error)
	Begin() interface{}
	Commit(ses db.Session, tctx interface{}) error
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

var (
	mutex     sync.RWMutex
	engines   = map[string]Engine{}
	databases = map[sql.Identifier]*databaseEntry{}

	dataDir = config.Var(new(string), "data_directory").
		Flag("data", "`directory` containing databases").NoConfig().String("testdata")
)

func newDatabaseEntry(eng string, name sql.Identifier, options Options,
	state databaseState) (Engine, *databaseEntry, error) {

	mutex.Lock()
	defer mutex.Unlock()

	if _, ok := databases[name]; ok {
		return nil, nil, fmt.Errorf("engine: database already exists: %s", name)
	}

	typ, ok := options[sql.ENGINE]
	if !ok {
		typ = eng
	} else {
		delete(options, sql.ENGINE)
	}
	e, ok := engines[typ]
	if !ok {
		return nil, nil, fmt.Errorf("engine: type not found: %s", typ)
	}
	path, ok := options[sql.PATH]
	if !ok {
		path = filepath.Join(*dataDir, name.String())
	} else {
		delete(options, sql.PATH)
	}
	de := &databaseEntry{
		state: state,
		name:  name,
		path:  path,
		typ:   typ,
	}
	databases[name] = de
	return e, de, nil
}

func setupDatabase(e Engine, de *databaseEntry, options Options) {
	var d Database
	if de.state == Attaching {
		d, de.err = e.AttachDatabase(de.name, de.path, options)
	} else {
		// de.state == Creating
		d, de.err = e.CreateDatabase(de.name, de.path, options)
	}

	mutex.Lock()
	defer mutex.Unlock()

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

func prepareDatabase(eng string, name sql.Identifier, options Options, state databaseState) error {
	e, de, err := newDatabaseEntry(eng, name, options, state)
	if err != nil {
		return err
	}

	_, ok := options[sql.WAIT]
	if ok {
		delete(options, sql.WAIT)
		setupDatabase(e, de, options)
		return de.err
	}

	go setupDatabase(e, de, options)
	return nil
}

func AttachDatabase(eng string, name sql.Identifier, options Options) error {
	return prepareDatabase(eng, name, options, Attaching)
}

func CreateDatabase(eng string, name sql.Identifier, options Options) error {
	return prepareDatabase(eng, name, options, Creating)
}

func DetachDatabase(name sql.Identifier) error {
	return nil // XXX
}

type Transaction struct {
	contexts map[Database]interface{}
}

// Begin a new transaction.
func Begin() *Transaction {
	return &Transaction{
		contexts: map[Database]interface{}{},
	}
}

func (tx *Transaction) forContexts(fn func(d Database, tctx interface{}) error) error {
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

func (tx *Transaction) Commit(ses db.Session) error {
	return tx.forContexts(func(d Database, tctx interface{}) error {
		return d.Commit(ses, tctx)
	})
}

func (tx *Transaction) Rollback() error {
	return tx.forContexts(func(d Database, tctx interface{}) error {
		return d.Rollback(tctx)
	})
}

func (tx *Transaction) NextStmt() {
	tx.forContexts(func(d Database, tctx interface{}) error {
		d.NextStmt(tctx)
		return nil
	})
}

func (tx *Transaction) getContext(d Database) interface{} {
	tctx, ok := tx.contexts[d]
	if !ok {
		tctx = d.Begin()
		tx.contexts[d] = tctx
	}
	return tctx
}

// LookupTable looks up the named table in the named database.
func LookupTable(ses db.Session, tx *Transaction, dbname, tblname sql.Identifier) (db.Table,
	error) {

	mutex.RLock()
	defer mutex.RUnlock()

	if dbname == 0 {
		dbname = ses.DefaultDatabase()
	}
	de, ok := databases[dbname]
	if !ok {
		return nil, fmt.Errorf("engine: database %s not found", dbname)
	}
	if de.state != Running {
		return nil, fmt.Errorf("engine: database %s not running", dbname)
	}
	tctx := tx.getContext(de.database)
	tbl, err := lookupVirtual(ses, tctx, de.database, tblname)
	if tbl != nil || err != nil {
		return tbl, err
	}
	return de.database.LookupTable(ses, tctx, tblname)
}

// CreateTable creates the named table in the named database.
func CreateTable(ses db.Session, tx *Transaction, dbname, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	mutex.RLock()
	defer mutex.RUnlock()

	if dbname == 0 {
		dbname = ses.DefaultDatabase()
	}
	de, ok := databases[dbname]
	if !ok {
		return fmt.Errorf("engine: database %s not found", dbname)
	}
	if de.state != Running {
		return fmt.Errorf("engine: database %s not running", dbname)
	}
	return de.database.CreateTable(ses, tx.getContext(de.database), tblname, cols, colTypes)
}

// DropTable drops the named table from the named database.
func DropTable(ses db.Session, tx *Transaction, dbname, tblname sql.Identifier, exists bool) error {
	mutex.RLock()
	defer mutex.RUnlock()

	if dbname == 0 {
		dbname = ses.DefaultDatabase()
	}
	de, ok := databases[dbname]
	if !ok {
		return fmt.Errorf("engine: database %s not found", dbname)
	}
	if de.state != Running {
		return fmt.Errorf("engine: database %s not running", dbname)
	}
	return de.database.DropTable(ses, tx.getContext(de.database), tblname, exists)
}

func Register(typ string, e Engine) {
	mutex.Lock()
	defer mutex.Unlock()

	if _, dup := engines[typ]; dup {
		panic(fmt.Sprintf("engine already registered: %s", typ))
	}
	engines[typ] = e
}
