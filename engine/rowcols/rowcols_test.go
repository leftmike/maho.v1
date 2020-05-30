package rowcols_test

import (
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/rowcols"
	"github.com/leftmike/maho/engine/test"
	"github.com/leftmike/maho/testutil"
)

func TestRowCols(t *testing.T) {
	path := filepath.Join("testdata", "rowcols")
	err := testutil.CleanDir(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	e, err := rowcols.NewEngine(path)
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
	err := testutil.CleanDir(filepath.Join("testdata", "rowcols"), nil)
	if err != nil {
		t.Fatal(err)
	}

	test.DurableTableLifecycleTest(t)
}

func TestDurableHelper(t *testing.T) {
	test.DurableHelper(t,
		func() (engine.Engine, error) {
			e, err := rowcols.NewEngine(filepath.Join("testdata", "rowcols"))
			if err != nil {
				return nil, err
			}
			return e, nil
		})
}
