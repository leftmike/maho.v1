package testutil

import (
	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/oldeng"
	"github.com/leftmike/maho/store"
	_ "github.com/leftmike/maho/store/test"
)

// StartEngine creates a test database and starts an engine; it is intended for use by testing.
func StartEngine(def string) (*oldeng.Engine, db.Database, error) {
	dbase, err := store.Open("test", def)
	if err != nil {
		return nil, nil, err
	}
	e, err := oldeng.Start(dbase)
	if err != nil {
		return nil, nil, err
	}

	return e, dbase, nil
}
