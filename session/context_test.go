package session_test

import (
	"testing"

	"github.com/leftmike/maho/session"
	"github.com/leftmike/maho/sql"
)

func TestContext(t *testing.T) {
	ctx := session.NewContext("engine", sql.ID("name"))
	if _, ok := ctx.(session.Context); !ok {
		t.Errorf("NewContext() got %T want session.Context", ctx)
	}
	if ctx.DefaultEngine() != "engine" {
		t.Errorf("DefaultEngine() got %q want %q", ctx.DefaultEngine(), "engine")
	}
	if ctx.DefaultDatabase() != sql.ID("name") {
		t.Errorf("DefaultDatabase() got %s want %s", ctx.DefaultDatabase(), sql.ID("name"))
	}
}
