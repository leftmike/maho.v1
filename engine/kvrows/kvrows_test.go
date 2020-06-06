package kvrows_test

import (
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/test"
	"github.com/leftmike/maho/testutil"
)

func TestBadgerKVRows(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	e, err := kvrows.NewBadgerEngine("testdata")
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

func TestBadgerDurability(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	test.DurableTests(t, "TestBadgerHelper")
}

func TestBadgerHelper(t *testing.T) {
	test.DurableHelper(t,
		func() (engine.Engine, error) {
			e, err := kvrows.NewBadgerEngine("testdata")
			if err != nil {
				return nil, err
			}
			return e, nil
		})
}
