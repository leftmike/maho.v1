package memrows_test

import (
	"testing"

	"github.com/leftmike/maho/engine/memrows"
	"github.com/leftmike/maho/engine/test"
)

func TestMemRows(t *testing.T) {
	e, err := memrows.NewEngine("testdata")
	if err != nil {
		t.Fatal(err)
	}
	test.RunDatabaseTest(t, e)
	test.RunSchemaTest(t, e)
	test.RunTableTest(t, e)
	test.RunParallelTest(t, e)
	test.RunStressTest(t, e)
}
