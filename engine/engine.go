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
	Dropping
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
	case Dropping:
		return "dropping"
	case Running:
		return "running"
	default:
		panic(fmt.Sprintf("unexpected value for database state: %d", ds))
	}
}

type Database interface {
	Type() string
	State() DatabaseState
	Path() string
	Message() string
	LookupTable(ctx context.Context, tx Transaction, tblname sql.Identifier) (db.Table, error)
	CreateTable(ctx context.Context, tx Transaction, tblname sql.Identifier,
		cols []sql.Identifier, colTypes []db.ColumnType) error
	DropTable(ctx context.Context, tx Transaction, tblname sql.Identifier, exists bool) error
	ListTables(ctx context.Context, tx Transaction) ([]TableEntry, error)
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
	databases = map[sql.Identifier]Database{}

	dataDir = config.Var(new(string), "data_directory").
		Flag("data", "`directory` containing databases").NoConfig().String("testdata")
)

type newDbFunc func(e Engine, name sql.Identifier, path string, options Options) (Database, error)

func newDatabase(eng string, name sql.Identifier, options Options, fn newDbFunc) error {
	mutex.Lock()
	defer mutex.Unlock()

	if _, ok := databases[name]; ok {
		return fmt.Errorf("engine: database already exists: %s", name)
	}

	typ, ok := options[sql.ENGINE]
	if !ok {
		typ = eng
	} else {
		delete(options, sql.ENGINE)
	}
	e, ok := engines[typ]
	if !ok {
		return fmt.Errorf("engine: type not found: %s", typ)
	}
	path, ok := options[sql.PATH]
	if !ok {
		path = filepath.Join(*dataDir, name.String())
	} else {
		delete(options, sql.PATH)
	}
	d, err := fn(e, name, path, options)
	if err != nil {
		return err
	}
	databases[name] = d
	return nil
}

func AttachDatabase(eng string, name sql.Identifier, options Options) error {
	return newDatabase(eng, name, options,
		func(e Engine, name sql.Identifier, path string, options Options) (Database, error) {
			return e.AttachDatabase(name, path, options)
		})
}

func CreateDatabase(eng string, name sql.Identifier, options Options) error {
	return newDatabase(eng, name, options,
		func(e Engine, name sql.Identifier, path string, options Options) (Database, error) {
			return e.CreateDatabase(name, path, options)
		})
}

func DetachDatabase(name sql.Identifier) error {
	return nil // XXX
}

func Use(name sql.Identifier) error {
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
	d, ok := databases[dbname]
	if !ok {
		return nil, fmt.Errorf("engine: database %s not found", dbname)
	}
	return d.LookupTable(ctx, tx, tblname)
}

// CreateTable creates the named table in the named database.
func CreateTable(ctx context.Context, tx Transaction, dbname, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	mutex.RLock()
	defer mutex.RUnlock()

	if dbname == 0 {
		dbname = tx.DefaultDatabase()
	}
	d, ok := databases[dbname]
	if !ok {
		return fmt.Errorf("engine: database %s not found", dbname)
	}
	return d.CreateTable(ctx, tx, tblname, cols, colTypes)
}

// DropTable drops the named table from the named database.
func DropTable(ctx context.Context, tx Transaction, dbname, tblname sql.Identifier,
	exists bool) error {

	mutex.RLock()
	defer mutex.RUnlock()

	if dbname == 0 {
		dbname = tx.DefaultDatabase()
	}
	d, ok := databases[dbname]
	if !ok {
		return fmt.Errorf("engine: database %s not found", dbname)
	}
	return d.DropTable(ctx, tx, tblname, exists)
}

func Register(typ string, e Engine) {
	mutex.Lock()
	defer mutex.Unlock()

	if _, dup := engines[typ]; dup {
		panic(fmt.Sprintf("engine already registered: %s", typ))
	}
	engines[typ] = e
}
