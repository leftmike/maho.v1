package kvrows_test

import (
	"testing"

	"github.com/leftmike/maho/storage/kvrows"
)

func TestBTreeKVStore(t *testing.T) {
	kv, err := kvrows.MakeBTreeKV()
	if err != nil {
		t.Fatal(err)
	}

	testKV(t, kv)
}
