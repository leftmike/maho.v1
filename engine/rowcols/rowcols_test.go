package rowcols_test

import (
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/rowcols"
	"github.com/leftmike/maho/engine/test"
	"github.com/leftmike/maho/testutil"
)

func TestRowCols(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	e, err := rowcols.NewEngine("testdata")
	if err != nil {
		t.Fatal(err)
	}
	test.RunDatabaseTest(t, e)
	test.RunTableTest(t, e)
	test.RunSchemaTest(t, e)
	test.RunTableLifecycleTest(t, e)
	test.RunTableRowsTest(t, e)
	test.RunStressTest(t, e)
	test.RunParallelTest(t, e)
}

func TestDurability(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	test.DurableTests(t, "TestRowColsHelper")
}

func TestRowColsHelper(t *testing.T) {
	test.DurableHelper(t,
		func() (engine.Engine, error) {
			e, err := rowcols.NewEngine("testdata")
			if err != nil {
				return nil, err
			}
			return e, nil
		})
}
