package basic

import (
	"github.com/leftmike/maho/store"
	"github.com/leftmike/maho/store/test"
)

func init() {
	store.Register("basic", test.TestStore{"basic"})
}
