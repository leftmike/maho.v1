package basic_test

import (
	"testing"

	"github.com/leftmike/maho/storage/basic"
	"github.com/leftmike/maho/storage/test"
)

func TestBasic(t *testing.T) {
	st, err := basic.NewStore("testdata")
	if err != nil {
		t.Fatal(err)
	}
	test.RunDatabaseTest(t, st)
	test.RunTableTest(t, st)
	test.RunSchemaTest(t, st)
	test.RunTableLifecycleTest(t, st)
	test.RunTableRowsTest(t, st)
	test.RunIndexLifecycleTest(t, st)
	test.RunIndexOneColUniqueTest(t, st)
	test.RunIndexTwoColUniqueTest(t, st)
	test.RunIndexOneColTest(t, st)
	test.RunIndexTwoColTest(t, st)
	test.RunPrimaryMinMaxTest(t, st)
	test.RunIndexMinMaxTest(t, st)
	test.RunStressTest(t, st)
	test.RunParallelTest(t, st)
}
