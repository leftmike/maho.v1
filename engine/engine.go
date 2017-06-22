package engine

import (
	"fmt"
	"maho/sql"
	"maho/store"
)

type Engine struct {
	databases       map[sql.Identifier]store.Database
	defaultDatabase sql.Identifier
}

func Start(db store.Database) (*Engine, error) {
	if db.Name() == sql.ENGINE {
		return nil, fmt.Errorf("engine: \"%s\" not allowed as database name", db.Name())
	}

	e := &Engine{make(map[sql.Identifier]store.Database), db.Name()}
	e.databases[db.Name()] = db
	e.databases[sql.ENGINE] = &engineDatabase{e}
	return e, nil
}

func (e *Engine) lookupTable(db, tbl sql.Identifier) (store.Table, error) {
	if db == 0 {
		db = e.defaultDatabase
	}
	s, ok := e.databases[db]
	if !ok {
		return nil, fmt.Errorf("engine: database \"%s\" not found", db)
	}
	return s.Table(tbl)
}
