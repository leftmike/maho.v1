package memkv_test

import (
	"testing"

	"github.com/leftmike/maho/engine/memkv"
	"github.com/leftmike/maho/engine/test"
)

func TestStore(t *testing.T) {
	test.RunLocalKVTest(t, memkv.OpenStore())
}
