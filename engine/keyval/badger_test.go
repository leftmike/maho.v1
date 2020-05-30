package keyval_test

import (
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/engine/keyval"
	"github.com/leftmike/maho/testutil"
)

func TestBadgerKV(t *testing.T) {
	path := filepath.Join("testdata", "badger")
	err := testutil.CleanDir(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	kv, err := keyval.MakeBadgerKV(path)
	if err != nil {
		t.Fatal(err)
	}
	testKV(t, kv)
}
