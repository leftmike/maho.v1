package basic_test

import (
	"testing"

	"github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/engine/test"
)

func TestBasic(t *testing.T) {
	e := &basic.Engine{}
	test.RunDatabaseTest(t, e)
	test.RunTableTest(t, e)
	test.RunParallelTest(t, e)
}
