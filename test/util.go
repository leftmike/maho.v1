package test

import (
	"maho/db"
	"maho/engine"
	"maho/sql"
	"maho/store"
	_ "maho/store/test"
)

type AllRows interface {
	AllRows() [][]sql.Value
}

func StartEngine(tbl string) (*engine.Engine, db.Database, error) {
	dbase, err := store.Open("test", tbl)
	if err != nil {
		return nil, nil, err
	}
	e, err := engine.Start(dbase)
	if err != nil {
		return nil, nil, err
	}

	return e, dbase, nil
}
