package test

import (
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/flags"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage/basic"
)

func StartSession(t *testing.T) (sql.Engine, *evaluate.Session) {
	t.Helper()

	st, err := basic.NewStore("testdata")
	if err != nil {
		t.Fatal(err)
	}
	e := engine.NewEngine(st, flags.Default())

	err = e.CreateDatabase(sql.ID("test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	return e, evaluate.NewSession(e, sql.ID("test"), sql.PUBLIC)
}
