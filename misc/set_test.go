package misc_test

import (
	"testing"

	"github.com/leftmike/maho/misc"
	"github.com/leftmike/maho/sql"
)

func TestSet(t *testing.T) {
	cases := []struct {
		s misc.Set
		r string
	}{
		{
			misc.Set{sql.ID("variable"), "value"},
			"SET variable TO value",
		},
	}

	for _, c := range cases {
		if c.s.String() != c.r {
			t.Errorf("Set{%v}.String() got %s want %s", c.s, c.s.String(), c.r)
		}
	}
}