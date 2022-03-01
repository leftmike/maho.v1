package kvrows_test

import (
	"testing"

	"github.com/leftmike/maho/storage/kvrows"
	"github.com/leftmike/maho/testutil"
)

func TestBBoltKVStore(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	kv, err := kvrows.MakeBBoltKV("testdata")
	if err != nil {
		t.Fatal(err)
	}

	testKV(t, kv)
}
