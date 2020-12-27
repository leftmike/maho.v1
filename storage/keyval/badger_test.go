package keyval_test

import (
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/storage/keyval"
	"github.com/leftmike/maho/testutil"
)

func TestBadgerKV(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	kv, err := keyval.MakeBadgerKV("testdata",
		testutil.SetupLogger(filepath.Join("testdata", "badger_kv.log")))
	if err != nil {
		t.Fatal(err)
	}
	testKV(t, kv)
}
