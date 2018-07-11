package basic_test

import (
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/test"
)

func TestBasic(t *testing.T) {
	e := engine.GetEngine("basic")
	test.RunDatabaseTest(t, e)
	test.RunTableTest(t, e)
	test.RunParallelTest(t, e)
}
