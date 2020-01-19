package badger_test

import (
	"testing"

	"github.com/leftmike/maho/engine/badger"
	"github.com/leftmike/maho/engine/test"
	"github.com/leftmike/maho/testutil"
)

func TestKV(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

	e, err := badger.NewEngine("testdata")
	if err != nil {
		t.Fatal(err)
	}

	test.RunDatabaseTest(t, e, true)
	test.RunTableTest(t, e)
	test.RunTableLifecycleTest(t, e)
	test.RunTableRowsTest(t, e)
	/*
		XXX
		test.RunSchemaTest(t, e)
		test.RunParallelTest(t, e)
		test.RunStressTest(t, e)
	*/
}
