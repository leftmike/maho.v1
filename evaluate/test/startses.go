package test

import (
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage/basic"
)

func StartSession(t *testing.T) (sql.Engine, *evaluate.Session) {
	t.Helper()

	st, err := basic.NewStore("testdata")
	if err != nil {
		t.Fatal(err)
	}
	e := engine.NewEngine(st)

	err = e.CreateDatabase(sql.ID("test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	return e, &evaluate.Session{
		Engine:          e,
		DefaultDatabase: sql.ID("test"),
		DefaultSchema:   sql.PUBLIC,
	}
}
