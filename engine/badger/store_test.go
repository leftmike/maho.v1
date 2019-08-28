package badger

import (
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/test"
	"github.com/leftmike/maho/testutil"
)

func runTest(t *testing.T, rtf func(t *testing.T, st kvrows.Store)) {
	t.Helper()

	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

	st, err := openStore(filepath.Join("testdata", "teststore"))
	if err != nil {
		t.Fatal(err)
	}

	rtf(t, st)
}

func TestStore(t *testing.T) {
	runTest(t, test.RunStoreTest)
}

func TestVersionedTable(t *testing.T) {
	runTest(t, test.RunVersionedTableTest)
}

func TestTransactedTable(t *testing.T) {
	runTest(t, test.RunTransactedTableTest)
}
