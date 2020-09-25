package rowcols_test

import (
	"testing"

	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/rowcols"
	"github.com/leftmike/maho/storage/test"
	"github.com/leftmike/maho/testutil"
)

func TestRowCols(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	st, err := rowcols.NewStore("testdata")
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

func TestDurability(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	test.DurableTests(t, "TestRowColsHelper")
}

func TestRowColsHelper(t *testing.T) {
	test.DurableHelper(t,
		func() (*storage.Store, error) {
			st, err := rowcols.NewStore("testdata")
			if err != nil {
				return nil, err
			}
			return st, nil
		})
}
