package bbolt

import (
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/engine/test"
	"github.com/leftmike/maho/testutil"
)

func TestStore(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

	st, err := openStore(filepath.Join("testdata", "teststore"))
	if err != nil {
		t.Fatal(err)
	}
	test.RunStoreTest(t, st)
}
