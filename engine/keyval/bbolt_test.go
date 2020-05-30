package keyval_test

import (
	"testing"

	"github.com/leftmike/maho/engine/keyval"
	"github.com/leftmike/maho/testutil"
)

func TestBBoltKV(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	kv, err := keyval.MakeBBoltKV("testdata")
	if err != nil {
		t.Fatal(err)
	}
	testKV(t, kv)
}
