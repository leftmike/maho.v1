package memrows_test

import (
	"testing"

	"github.com/leftmike/maho/engine/memrows"
	"github.com/leftmike/maho/engine/test"
)

func TestMemRows(t *testing.T) {
	e := &memrows.Engine{}
	test.RunDatabaseTest(t, e)
	test.RunTableTest(t, e)
	test.RunParallelTest(t, e)
	test.RunStressTest(t, e)
}
