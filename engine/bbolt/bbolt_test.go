package bbolt_test

import (
	"testing"

	"github.com/leftmike/maho/engine/bbolt"
	"github.com/leftmike/maho/engine/test"
	"github.com/leftmike/maho/testutil"
)

func TestBBolt(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

	e, err := bbolt.NewEngine("testdata")
	if err != nil {
		t.Fatal(err)
	}
	test.RunDatabaseTest(t, e, false) // XXX: should be true
	//test.RunTableTest(t, e)
	/*
		XXX
		test.RunTableLifecycleTest(t, e)
		test.RunSchemaTest(t, e)
		test.RunTableDataTest(t, e)
		test.RunParallelTest(t, e)
		test.RunStressTest(t, e)
	*/
}
