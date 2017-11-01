package test

import (
	"io"
	"maho/db"
	"maho/engine"
	"maho/sql"
	"maho/store"
	_ "maho/store/test"
)

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
