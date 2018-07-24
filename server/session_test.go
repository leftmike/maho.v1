package server_test

import (
	"testing"

	"github.com/leftmike/maho/server"
	"github.com/leftmike/maho/sql"
)

func TestContext(t *testing.T) {
	ses := server.NewSession(nil, "engine", sql.ID("name"))
	if ses.DefaultEngine() != "engine" {
		t.Errorf("DefaultEngine() got %q want %q", ses.DefaultEngine(), "engine")
	}
	if ses.DefaultDatabase() != sql.ID("name") {
		t.Errorf("DefaultDatabase() got %s want %s", ses.DefaultDatabase(), sql.ID("name"))
	}
}
