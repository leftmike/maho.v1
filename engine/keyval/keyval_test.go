package keyval_test

import (
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/testutil"
)

func TestKeyVal(t *testing.T) {
	path := filepath.Join("testdata", "keyval")
	err := testutil.CleanDir(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	/*
		e, err := keyval.NewEngine(path)
		if err != nil {
			t.Fatal(err)
		}
		_ = e
	*/
	/*
		test.RunDatabaseTest(t, e)
		test.RunTableTest(t, e)
		test.RunSchemaTest(t, e)
		test.RunTableLifecycleTest(t, e)
		test.RunTableRowsTest(t, e)
		test.RunStressTest(t, e)
		test.RunParallelTest(t, e)
	*/
}
