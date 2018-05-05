package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

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

type Engine interface {
	CreateDatabase(path string) (Database, error)
	AttachDatabase(path string) (Database, error)
}

type Database interface {
	LookupTable(ctx context.Context, tx Transaction, tblname sql.Identifier) (db.Table, error)
	CreateTable(ctx context.Context, tx Transaction, tblname sql.Identifier,
		cols []sql.Identifier, colTypes []db.ColumnType) error
	DropTable(ctx context.Context, tx Transaction, tblname sql.Identifier, exists bool) error
	ListTables(ctx context.Context, tx Transaction) ([]TableEntry, error)
}

type Transaction interface {
	Commit(ctx context.Context) error
	Rollback() error
}

var (
	mutex     sync.RWMutex
	engines   = map[string]Engine{}
	databases = map[sql.Identifier]Database{}
	defaultDb sql.Identifier
)

// Start an engine of typ, use dir as the data directory, and attach or create the
// named database.
func Start(typ, dir string, dbname sql.Identifier) error {
	mutex.Lock()
	defer mutex.Unlock()

	e, ok := engines[typ]
	if !ok {
		return fmt.Errorf("engine: type not found: %s", typ)
	}
	d, err := e.AttachDatabase(filepath.Join(dir, dbname.String()))
	if err != nil {
		d, err = e.CreateDatabase(filepath.Join(dir, dbname.String()))
		if err != nil {
			return err
		}
	}
	databases[dbname] = d
	defaultDb = dbname
	return nil
}

type transaction struct{}

// Begin a new transaction.
func Begin() (Transaction, error) {
	return &transaction{}, nil
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
		dbname = defaultDb
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
		dbname = defaultDb
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
		dbname = defaultDb
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
