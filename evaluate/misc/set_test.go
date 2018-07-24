package misc_test

import (
	"testing"

	"github.com/leftmike/maho/evaluate/misc"
	"github.com/leftmike/maho/sql"
)

func TestSet(t *testing.T) {
	cases := []struct {
		s misc.Set
		r string
	}{
		{
			misc.Set{Variable: sql.ID("variable"), Value: "value"},
			"SET variable TO value",
		},
	}

	for _, c := range cases {
		if c.s.String() != c.r {
			t.Errorf("Set{%v}.String() got %s want %s", c.s, c.s.String(), c.r)
		}
	}
}
