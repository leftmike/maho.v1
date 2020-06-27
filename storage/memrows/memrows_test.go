package memrows_test

import (
	"testing"

	"github.com/leftmike/maho/storage/memrows"
	"github.com/leftmike/maho/storage/test"
)

func TestMemRows(t *testing.T) {
	st, err := memrows.NewStore("testdata")
	if err != nil {
		t.Fatal(err)
	}
	test.RunDatabaseTest(t, st)
	test.RunTableTest(t, st)
	test.RunTableLifecycleTest(t, st)
	test.RunSchemaTest(t, st)
	test.RunTableRowsTest(t, st)
	test.RunParallelTest(t, st)
	test.RunStressTest(t, st)
}
