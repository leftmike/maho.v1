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
	CreateDatabase(dbname string) error
	OpenDatabase(dbname string) (bool, error)

	// Lookup the named table in the named database.
	LookupTable(dbname string, tblname sql.Identifier) (db.Table, error)

	// Create the named table in the named database.
	CreateTable(dbname string, tblname sql.Identifier, cols []sql.Identifier,
		colTypes []db.ColumnType) error

	// Drop the named table from the named database.
	DropTable(dbname string, tblname sql.Identifier, exists bool) error
}

var (
	enginesMutex sync.Mutex
	engines      = map[string]Engine{}
	E            Engine
)

// Start an engine of typ, use dir as the data directory, and open or create the
// named database.
func Start(typ, dir, dbname string) error {
	enginesMutex.Lock()
	defer enginesMutex.Unlock()

	if E != nil {
		panic("engine already started")
	}
	e, ok := engines[typ]
	if !ok {
		return fmt.Errorf("engine: type not found: %s", typ)
	}
	err := e.Start(dir)
	if err != nil {
		return err
	}

	ok, err = e.OpenDatabase(dbname)
	if err != nil {
		return err
	}
	if !ok {
		err = e.CreateDatabase(dbname)
		if err != nil {
			return nil
		}
	}

	E = e
	return nil
}

func Register(typ string, e Engine) {
	enginesMutex.Lock()
	defer enginesMutex.Unlock()

	if _, dup := engines[typ]; dup {
		panic(fmt.Sprintf("engine already registered: %s", typ))
	}
	engines[typ] = e
}
