package kvrows_test

import (
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/kvrows"
	"github.com/leftmike/maho/storage/test"
	"github.com/leftmike/maho/testutil"
)

func TestBadgerKVRows(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	st, err := kvrows.NewBadgerStore("testdata",
		testutil.SetupLogger(filepath.Join("testdata", "badger_kvrows.log")))
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
			st, err := kvrows.NewBadgerStore("testdata",
				testutil.SetupLogger(filepath.Join("testdata", "badger_durable.log")))
			if err != nil {
				return nil, err
			}
			return st, nil
		})
}

func TestPebbleKVRows(t *testing.T) {
	dataDir := filepath.Join("testdata", "pebble_kvrows")
	err := testutil.CleanDir(dataDir, []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	st, err := kvrows.NewPebbleStore(dataDir,
		testutil.SetupLogger(filepath.Join("testdata", "pebble_kvrows.log")))
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

func TestPebbleDurability(t *testing.T) {
	err := testutil.CleanDir(filepath.Join("testdata", "pebble_durable"), []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	test.DurableTests(t, "TestPebbleHelper")
}

func TestPebbleHelper(t *testing.T) {
	test.DurableHelper(t,
		func() (*storage.Store, error) {
			st, err := kvrows.NewPebbleStore(filepath.Join("testdata", "pebble_durable"),
				testutil.SetupLogger(filepath.Join("testdata", "pebble_durable.log")))
			if err != nil {
				return nil, err
			}
			return st, nil
		})
}

func TestBBoltKVRows(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	st, err := kvrows.NewBBoltStore("testdata")
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
			st, err := kvrows.NewBBoltStore("testdata")
			if err != nil {
				return nil, err
			}
			return st, nil
		})
}
