package test

import (
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/engine/kv"
	"github.com/leftmike/maho/testutil"
)

func RunKVTest(t *testing.T, e kv.Engine) {
	t.Helper()

	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

	db, err := e.Open(filepath.Join("testdata", "testkv"))
	if err != nil {
		t.Fatalf("Open() failed with %s", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Close() failed with %s", err)
	}
}
