package memkv_test

import (
	"testing"

	"github.com/leftmike/maho/engine/memkv"
	"github.com/leftmike/maho/engine/test"
)

func TestKV(t *testing.T) {
	e, err := memkv.NewEngine("")
	if err != nil {
		t.Fatal(err)
	}
	test.RunDatabaseTest(t, e, true)
	test.RunTableTest(t, e)
	test.RunSchemaTest(t, e)
	test.RunTableLifecycleTest(t, e)
	test.RunTableRowsTest(t, e)
	/*
		XXX
		test.RunParallelTest(t, e)
		test.RunStressTest(t, e)
	*/
}
