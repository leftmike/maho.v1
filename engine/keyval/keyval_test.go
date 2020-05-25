package keyval_test

import (
	"testing"

	"github.com/leftmike/maho/testutil"
)

func TestKeyVal(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	/*
		e, err := keyval.NewEngine("testdata")
		if err != nil {
			t.Fatal(err)
		}
			test.RunDatabaseTest(t, e)
			test.RunTableTest(t, e)
			test.RunSchemaTest(t, e)
			test.RunTableLifecycleTest(t, e)
			test.RunTableRowsTest(t, e)
			test.RunStressTest(t, e)
			test.RunParallelTest(t, e)
	*/
}
