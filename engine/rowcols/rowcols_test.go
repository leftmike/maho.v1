package rowcols_test

import (
	"testing"

	"github.com/leftmike/maho/engine/rowcols"
	"github.com/leftmike/maho/engine/test"
)

func TestRowCols(t *testing.T) {
	e, err := rowcols.NewEngine("testdata")
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
