package engine

import (
	"fmt"
	"sync"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/sql"
)

type Engine interface {
	// Start the engine using dir as the data directory.
	Start(dir string) error
	CreateDatabase(dbname sql.Identifier) error
	OpenDatabase(dbname sql.Identifier) (bool, error)

	// Lookup the named table in the named database.
	LookupTable(dbname, tblname sql.Identifier) (db.Table, error)

	// Create the named table in the named database.
	CreateTable(dbname, tblname sql.Identifier, cols []sql.Identifier,
		colTypes []db.ColumnType) error

	// Drop the named table from the named database.
	DropTable(dbname, tblname sql.Identifier, exists bool) error
}

var (
	enginesMutex sync.Mutex
	engines      = map[string]Engine{}
	e            Engine
	defaultName  sql.Identifier
)

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

	ok, err = ne.OpenDatabase(dbname)
	if err != nil {
		return err
	}
	if !ok {
		err = ne.CreateDatabase(dbname)
		if err != nil {
			return nil
		}
	}

	e = ne
	defaultName = dbname
	return nil
}

// Lookup the named table in the named database.
func LookupTable(dbname, tblname sql.Identifier) (db.Table, error) {
	if e == nil {
		panic("start the engine first")
	}
	if dbname == 0 {
		dbname = defaultName
	}
	return e.LookupTable(dbname, tblname)
}

// Create the named table in the named database.
func CreateTable(dbname, tblname sql.Identifier, cols []sql.Identifier,
	colTypes []db.ColumnType) error {

	if e == nil {
		panic("start the engine first")
	}
	if dbname == 0 {
		dbname = defaultName
	}
	return e.CreateTable(dbname, tblname, cols, colTypes)
}

// Drop the named table from the named database.
func DropTable(dbname, tblname sql.Identifier, exists bool) error {
	if e == nil {
		panic("start the engine first")
	}
	if dbname == 0 {
		dbname = defaultName
	}
	return e.DropTable(dbname, tblname, exists)
}

func Register(typ string, e Engine) {
	enginesMutex.Lock()
	defer enginesMutex.Unlock()

	if _, dup := engines[typ]; dup {
		panic(fmt.Sprintf("engine already registered: %s", typ))
	}
	engines[typ] = e
}
