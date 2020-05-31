package keyval_test

import (
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/keyval"
	"github.com/leftmike/maho/engine/test"
	"github.com/leftmike/maho/testutil"
)

func TestBadgerKeyVal(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	e, err := keyval.NewBadgerEngine("testdata")
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
			e, err := keyval.NewBadgerEngine("testdata")
			if err != nil {
				return nil, err
			}
			return e, nil
		})
}

func TestBBoltKeyVal(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	e, err := keyval.NewBBoltEngine("testdata")
	if err != nil {
		t.Fatal(err)
	}
	test.RunDatabaseTest(t, e)
	test.RunTableTest(t, e)
	test.RunSchemaTest(t, e)
	test.RunTableLifecycleTest(t, e)
	test.RunTableRowsTest(t, e)
	// Work, but are very slow without setting NoSync = true and NoFreelistSync = true
	test.RunStressTest(t, e)
	test.RunParallelTest(t, e)
}

func TestBBoltDurability(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	test.DurableTests(t, "TestBBoltHelper")
}

func TestBBoltHelper(t *testing.T) {
	test.DurableHelper(t,
		func() (engine.Engine, error) {
			e, err := keyval.NewBBoltEngine("testdata")
			if err != nil {
				return nil, err
			}
			return e, nil
		})
}
