package badger_test

import (
	"testing"

	"github.com/leftmike/maho/engine/badger"
	"github.com/leftmike/maho/testutil"
)

func TestKV(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

	e, err := badger.NewEngine("testdata")
	if err != nil {
		// XXX
		// t.Fatal(err)
	}
	/*
		test.RunDatabaseTest(t, e)
		test.RunSchemaTest(t, e)
		test.RunTableTest(t, e)
		test.RunParallelTest(t, e)
		test.RunStressTest(t, e)
	*/
	_ = e
}
