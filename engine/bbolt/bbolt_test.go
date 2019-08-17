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
	_ = e
	test.RunDatabaseTest(t, e)
	/*
		test.RunSchemaTest(t, e)
		test.RunTableTest(t, e)
		test.RunParallelTest(t, e)
		test.RunStressTest(t, e)
	*/
}
