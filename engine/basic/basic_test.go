package basic_test

import (
	"testing"

	"github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/engine/test"
)

func TestBasic(t *testing.T) {
	e, err := basic.NewEngine("testdata")
	if err != nil {
		t.Fatal(err)
	}
	test.RunDatabaseTest(t, e, true)
	test.RunTableTest(t, e)
	test.RunSchemaTest(t, e)
	test.RunTableLifecycleTest(t, e)
	test.RunTableRowsTest(t, e)
	test.RunStressTest(t, e)
	test.RunParallelTest(t, e)
}
