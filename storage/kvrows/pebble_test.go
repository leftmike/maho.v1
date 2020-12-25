package kvrows_test

import (
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/storage/kvrows"
	"github.com/leftmike/maho/testutil"
)

func TestPebbleKV(t *testing.T) {
	dataDir := filepath.Join("testdata", "pebble_kv")
	err := testutil.CleanDir(dataDir, []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	kv, err := kvrows.MakePebbleKV(dataDir)
	if err != nil {
		t.Fatal(err)
	}

	testKV(t, kv)
}
