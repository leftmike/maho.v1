package engine

import (
	"context"
	"fmt"
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
	// Start the engine using dir as the data directory.
	Start(dir string) error
	CreateDatabase(dbname sql.Identifier) error
	ListDatabases() []sql.Identifier

	Begin() (Transaction, error)
}

type Transaction interface {
	LookupTable(ctx context.Context, dbname, tblname sql.Identifier) (db.Table, error)
	CreateTable(ctx context.Context, dbname, tblname sql.Identifier, cols []sql.Identifier,
		colTypes []db.ColumnType) error
	DropTable(ctx context.Context, dbname, tblname sql.Identifier, exists bool) error
	ListTables(ctx context.Context, dbname sql.Identifier) ([]TableEntry, error)

	Commit(ctx context.Context) error
	Rollback() error
}

var (
	enginesMutex sync.Mutex
	engines      = map[string]Engine{}
	e            Engine
	defaultName  sql.Identifier
)

func findDatabase(ids []sql.Identifier, dbname sql.Identifier) bool {
	for _, id := range ids {
		if id == dbname {
			return true
		}
	}
	return false
}

// Start an engine of typ, use dir as the data directory, and open or create the
// named database.
func Start(typ, dir string, dbname sql.Identifier) error {
	enginesMutex.Lock()
	defer enginesMutex.Unlock()

	if e != nil {
		panic("engine already started")
	}
	ne, ok := engines[typ]
	if !ok {
		return fmt.Errorf("engine: type not found: %s", typ)
	}
	err := ne.Start(dir)
	if err != nil {
		return err
	}

	if !findDatabase(ne.ListDatabases(), dbname) {
		err = ne.CreateDatabase(dbname)
		if err != nil {
			return err
		}
	}

	e = ne
	defaultName = dbname
	return nil
}

// Begin a new transaction.
func Begin() (Transaction, error) {
	if e == nil {
		panic("start the engine first")
	}
	return e.Begin()
}

// LookupTable looks up the named table in the named database.
func LookupTable(ctx context.Context, tx Transaction, dbname, tblname sql.Identifier) (db.Table,
	error) {

	if e == nil {
		panic("start the engine first")
	}
	if dbname == 0 {
		dbname = defaultName
	}
	tbl, err := lookupVirtual(ctx, tx, dbname, tblname)
	if tbl != nil || err != nil {
		return tbl, err
	}
	return tx.LookupTable(ctx, dbname, tblname)
}

// CreateTable creates the named table in the named database.
func CreateTable(ctx context.Context, tx Transaction, dbname, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	if e == nil {
		panic("start the engine first")
	}
	if dbname == 0 {
		dbname = defaultName
	}
	return tx.CreateTable(ctx, dbname, tblname, cols, colTypes)
}

// DropTable drops the named table from the named database.
func DropTable(ctx context.Context, tx Transaction, dbname, tblname sql.Identifier,
	exists bool) error {

	if e == nil {
		panic("start the engine first")
	}
	if dbname == 0 {
		dbname = defaultName
	}
	return tx.DropTable(ctx, dbname, tblname, exists)
}

func Register(typ string, e Engine) {
	enginesMutex.Lock()
	defer enginesMutex.Unlock()

	if _, dup := engines[typ]; dup {
		panic(fmt.Sprintf("engine already registered: %s", typ))
	}
	engines[typ] = e
}
