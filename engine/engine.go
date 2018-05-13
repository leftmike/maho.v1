package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/sql"
)

type TableID uint32
type PageNum uint64

type TableType int

const (
	PhysicalType TableType = iota
	VirtualType
)

type TableEntry struct {
	Name    sql.Identifier
	ID      TableID
	PageNum PageNum
	Type    TableType
}

type Options map[sql.Identifier]string

type Engine interface {
	AttachDatabase(name sql.Identifier, path string, options Options) (Database, error)
	CreateDatabase(name sql.Identifier, path string, options Options) (Database, error)
}

type DatabaseState int

const (
	Attaching DatabaseState = iota
	Creating
	Detaching
	ErrorAttaching
	ErrorCreating
	ErrorDetaching
	Running
)

func (ds DatabaseState) String() string {
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
	LookupTable(ctx context.Context, tx Transaction, tblname sql.Identifier) (db.Table, error)
	CreateTable(ctx context.Context, tx Transaction, tblname sql.Identifier,
		cols []sql.Identifier, colTypes []db.ColumnType) error
	DropTable(ctx context.Context, tx Transaction, tblname sql.Identifier, exists bool) error
	ListTables(ctx context.Context, tx Transaction) ([]TableEntry, error)
}

type databaseEntry struct {
	database Database
	state    DatabaseState
	name     sql.Identifier
	path     string
	typ      string
	err      error
}

type Transaction interface {
	DefaultEngine() string
	DefaultDatabase() sql.Identifier
	Commit(ctx context.Context) error
	Rollback() error
}

var (
	mutex     sync.RWMutex
	engines   = map[string]Engine{}
	databases = map[sql.Identifier]*databaseEntry{}

	dataDir = config.Var(new(string), "data_directory").
		Flag("data", "`directory` containing databases").NoConfig().String("testdata")
)

func newDatabaseEntry(eng string, name sql.Identifier, options Options,
	state DatabaseState) (Engine, *databaseEntry, error) {

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
		// de.state == creating
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

func prepareDatabase(eng string, name sql.Identifier, options Options, state DatabaseState) error {
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

type transaction struct {
	eng  string
	name sql.Identifier
}

// Begin a new transaction.
func Begin(eng string, name sql.Identifier) (Transaction, error) {
	return &transaction{
		eng:  eng,
		name: name,
	}, nil
}

func (tx *transaction) DefaultEngine() string {
	return tx.eng
}

func (tx *transaction) DefaultDatabase() sql.Identifier {
	return tx.name
}

func (tx *transaction) Commit(ctx context.Context) error {
	return nil
}

func (tx *transaction) Rollback() error {
	return nil
}

// LookupTable looks up the named table in the named database.
func LookupTable(ctx context.Context, tx Transaction, dbname, tblname sql.Identifier) (db.Table,
	error) {

	mutex.RLock()
	defer mutex.RUnlock()

	if dbname == 0 {
		dbname = tx.DefaultDatabase()
	}
	tbl, err := lookupVirtual(ctx, tx, dbname, tblname)
	if tbl != nil || err != nil {
		return tbl, err
	}
	de, ok := databases[dbname]
	if !ok {
		return nil, fmt.Errorf("engine: database %s not found", dbname)
	}
	if de.state != Running {
		return nil, fmt.Errorf("engine: database %s not running", dbname)
	}
	return de.database.LookupTable(ctx, tx, tblname)
}

// CreateTable creates the named table in the named database.
func CreateTable(ctx context.Context, tx Transaction, dbname, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	mutex.RLock()
	defer mutex.RUnlock()

	if dbname == 0 {
		dbname = tx.DefaultDatabase()
	}
	de, ok := databases[dbname]
	if !ok {
		return fmt.Errorf("engine: database %s not found", dbname)
	}
	if de.state != Running {
		return fmt.Errorf("engine: database %s not running", dbname)
	}
	return de.database.CreateTable(ctx, tx, tblname, cols, colTypes)
}

// DropTable drops the named table from the named database.
func DropTable(ctx context.Context, tx Transaction, dbname, tblname sql.Identifier,
	exists bool) error {

	mutex.RLock()
	defer mutex.RUnlock()

	if dbname == 0 {
		dbname = tx.DefaultDatabase()
	}
	de, ok := databases[dbname]
	if !ok {
		return fmt.Errorf("engine: database %s not found", dbname)
	}
	if de.state != Running {
		return fmt.Errorf("engine: database %s not running", dbname)
	}
	return de.database.DropTable(ctx, tx, tblname, exists)
}

func Register(typ string, e Engine) {
	mutex.Lock()
	defer mutex.Unlock()

	if _, dup := engines[typ]; dup {
		panic(fmt.Sprintf("engine already registered: %s", typ))
	}
	engines[typ] = e
}
