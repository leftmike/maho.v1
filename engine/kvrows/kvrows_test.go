package kvrows_test

import (
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/badger"
	"github.com/leftmike/maho/engine/bbolt"
	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/test"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

func testKVRows(t *testing.T, e engine.Engine) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

	var svcs test.Services
	svcs.Init()
	d, err := e.CreateDatabase(&svcs, sql.ID("test"), filepath.Join("testdata", "test"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !d.CanClose(false) {
		t.Error("CanClose(false) got false want true")
	}
	if !d.CanClose(true) {
		t.Error("CanClose(true) got false want true")
	}
	err = d.Close(false)
	if err != nil {
		t.Fatal(err)
	}
}

func TestKVRows(t *testing.T) {
	badgerEngine := kvrows.Engine{Engine: badger.Engine{}}
	bboltEngine := kvrows.Engine{Engine: bbolt.Engine{}}
	testKVRows(t, badgerEngine)
	testKVRows(t, bboltEngine)

	test.RunDatabaseTest(t, badgerEngine)
	test.RunDatabaseTest(t, bboltEngine)

	// XXX
	//test.RunTableTest(t, badgerEngine)
	//test.RunTableTest(t, bboltEngine)

	//test.RunParallelTest(t, badgerEngine)
	//test.RunParallelTest(t, bboltEngine)

	//test.RunStressTest(t, badgerEngine)
	//test.RunStressTest(t, bboltEngine)
}
