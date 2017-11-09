package testutil

import (
	"io"
	"maho/db"
	"maho/engine"
	"maho/sql"
	"maho/store"
	_ "maho/store/test"
)

// StartEngine creates a test database and starts an engine; it is intended for use by testing.
func StartEngine(def string) (*engine.Engine, db.Database, error) {
	dbase, err := store.Open("test", def)
	if err != nil {
		return nil, nil, err
	}
	e, err := engine.Start(dbase)
	if err != nil {
		return nil, nil, err
	}

	return e, dbase, nil
}

// AllRows returns all of the rows from a db.Rows as slices of values.
func AllRows(rows db.Rows) ([][]sql.Value, error) {
	all := [][]sql.Value{}
	l := len(rows.Columns())
	for {
		dest := make([]sql.Value, l)
		err := rows.Next(dest)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		all = append(all, dest)
	}
	return all, nil
}
