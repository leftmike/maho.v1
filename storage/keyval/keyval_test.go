package keyval_test

import (
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/keyval"
	"github.com/leftmike/maho/storage/test"
	"github.com/leftmike/maho/testutil"
)

func TestBadgerKeyVal(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	st, err := keyval.NewBadgerStore("testdata",
		testutil.SetupLogger(filepath.Join("testdata", "badger_keyval.log")))
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

	test.RunGuardTest(t, st)
	test.RunStressTest(t, st)
	test.RunParallelTest(t, st)
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
		func() (*storage.Store, error) {
			st, err := keyval.NewBadgerStore("testdata",
				testutil.SetupLogger(filepath.Join("testdata", "badger_durable.log")))
			if err != nil {
				return nil, err
			}
			return st, nil
		})
}

func TestBBoltKeyVal(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	st, err := keyval.NewBBoltStore("testdata")
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

	test.RunGuardTest(t, st)
	// Work, but are very slow without setting NoSync = true and NoFreelistSync = true
	test.RunStressTest(t, st)
	test.RunParallelTest(t, st)
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
		func() (*storage.Store, error) {
			st, err := keyval.NewBBoltStore("testdata")
			if err != nil {
				return nil, err
			}
			return st, nil
		})
}
