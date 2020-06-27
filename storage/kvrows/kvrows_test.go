package kvrows_test

import (
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

	st, err := kvrows.NewBadgerStore("testdata")
	if err != nil {
		t.Fatal(err)
	}
	test.RunDatabaseTest(t, st)
	test.RunTableTest(t, st)
	test.RunSchemaTest(t, st)
	test.RunTableLifecycleTest(t, st)
	test.RunTableRowsTest(t, st)
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
		func() (storage.Store, error) {
			st, err := kvrows.NewBadgerStore("testdata")
			if err != nil {
				return nil, err
			}
			return st, nil
		})
}
