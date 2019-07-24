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

	e := bbolt.NewEngine("testdata")
	test.RunDatabaseTest(t, e)
	test.RunSchemaTest(t, e)
	/*
		test.RunTableTest(t, e) // XXX
		test.RunParallelTest(t, e) // XXX
		test.RunStressTest(t, e) // XXX
	*/
}
