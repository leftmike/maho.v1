package execute_test

import (
	"testing"

	"github.com/leftmike/maho/execute"
	"github.com/leftmike/maho/sql"
)

func TestContext(t *testing.T) {
	ses := execute.NewSession("engine", sql.ID("name"))
	if ses.DefaultEngine() != "engine" {
		t.Errorf("DefaultEngine() got %q want %q", ses.DefaultEngine(), "engine")
	}
	if ses.DefaultDatabase() != sql.ID("name") {
		t.Errorf("DefaultDatabase() got %s want %s", ses.DefaultDatabase(), sql.ID("name"))
	}
}
