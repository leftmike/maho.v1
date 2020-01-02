package kvrows_test

import (
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/engine/badger"
	"github.com/leftmike/maho/engine/bbolt"
	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/localkv"
	"github.com/leftmike/maho/engine/memkv"
	"github.com/leftmike/maho/engine/test"
	"github.com/leftmike/maho/testutil"
)

func testEngine(t *testing.T, st kvrows.Store) {
	t.Helper()

	var kv kvrows.KVRows
	err := kv.Startup(st)
	if err != nil {
		t.Fatalf("kv.Startup() failed with %s", err)
	}

	test.RunDatabaseTest(t, &kv, true)
	test.RunTableTest(t, &kv)
	test.RunSchemaTest(t, &kv)
	test.RunTableLifecycleTest(t, &kv)
	/*
		XXX
		test.RunTableRowsTest(t, &kv)
		test.RunParallelTest(t, &kv)
		test.RunStressTest(t, &kv)
	*/
}

func TestBadger(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

	st, err := badger.OpenStore(filepath.Join("testdata", "teststore"))
	if err != nil {
		t.Fatal(err)
	}
	testEngine(t, localkv.NewStore(st))
}

func TestBBolt(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

	st, err := bbolt.OpenStore(filepath.Join("testdata", "teststore"))
	if err != nil {
		t.Fatal(err)
	}
	testEngine(t, localkv.NewStore(st))
}

func TestMemKV(t *testing.T) {
	testEngine(t, localkv.NewStore(memkv.OpenStore()))
}
