package kvrows_test

import (
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/badger"
	"github.com/leftmike/maho/engine/bbolt"
	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/testutil"
)

func testKVRows(t *testing.T, e engine.Engine) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

}

func TestKVRows(t *testing.T) {
	// XXX: engine/test

	testKVRows(t, kvrows.Engine{Engine: badger.Engine{}})
	testKVRows(t, kvrows.Engine{Engine: bbolt.Engine{}})
}
