package badger_test

import (
	"testing"

	"github.com/leftmike/maho/engine/badger"
	"github.com/leftmike/maho/engine/test"
)

func TestKV(t *testing.T) {
	test.RunKVTest(t, badger.Engine{})
}
