package bbolt_test

import (
	"testing"

	"github.com/leftmike/maho/engine/bbolt"
	"github.com/leftmike/maho/engine/test"
)

func TestBBoltKV(t *testing.T) {
	test.RunKVTest(t, bbolt.Engine{})
}
