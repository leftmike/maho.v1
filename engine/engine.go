package engine

import (
	"fmt"
	"maho/sql"
	"maho/store"
)

type Engine struct {
	stores       map[sql.Identifier]store.Store
	defaultStore sql.Identifier
}

func Start(s store.Store) (*Engine, error) {
	if s.Name() == sql.ENGINE {
		return nil, fmt.Errorf("engine: \"%s\" not allowed as database name", s.Name())
	}

	e := &Engine{make(map[sql.Identifier]store.Store), s.Name()}
	e.stores[s.Name()] = s
	e.stores[sql.ENGINE] = &engineStore{e}
	return e, nil
}

func (e *Engine) lookupTable(db, tbl sql.Identifier) (store.Table, error) {
	if db == 0 {
		db = e.defaultStore
	}
	s, ok := e.stores[db]
	if !ok {
		return nil, fmt.Errorf("engine: database \"%s\" not found", db)
	}
	return s.Table(tbl)
}
