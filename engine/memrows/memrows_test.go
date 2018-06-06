package memrows_test

import (
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/test"
)

func TestMemRows(t *testing.T) {
	e := engine.GetEngine("memrows")
	test.RunDatabaseTest(t, e)
	test.RunTableTest(t, e)
}
