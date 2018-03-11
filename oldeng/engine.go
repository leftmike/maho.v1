package oldeng

import (
	"fmt"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/sql"
)

type Engine struct {
	databases       map[sql.Identifier]db.Database
	defaultDatabase sql.Identifier
}

func Start(dbase db.Database) (*Engine, error) {
	if dbase.Name() == sql.ENGINE {
		return nil, fmt.Errorf("engine: \"%s\" not allowed as database name", dbase.Name())
	}

	e := &Engine{make(map[sql.Identifier]db.Database), dbase.Name()}
	e.databases[dbase.Name()] = dbase
	e.databases[sql.ENGINE] = &engineDatabase{e}
	return e, nil
}

func (e *Engine) LookupDatabase(db sql.Identifier) (db.Database, error) {
	if db == 0 {
		db = e.defaultDatabase
	}
	s, ok := e.databases[db]
	if !ok {
		return nil, fmt.Errorf("engine: database \"%s\" not found", db)
	}
	return s, nil
}
