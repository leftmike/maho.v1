package sql_test

import (
	"testing"

	"maho/sql"
)

func TestLess(t *testing.T) {
	cases := []struct {
		v1, v2 sql.Value
		less   bool
	}{
		{nil, true, true},
		{nil, nil, false},

		{false, nil, false},
		{true, true, false},
		{false, false, false},
		{false, true, true},
		{true, false, false},
		{false, float64(1.23), true},

		{float64(1.23), false, false},
		{float64(1.23), int64(123), true},
		{float64(1.23), "abc", true},
		{float64(1.23), float64(2.34), true},
		{float64(1.23), float64(1.23), false},
		{float64(1.23), float64(0.12), false},

		{int64(123), false, false},
		{int64(123), float64(1.23), false},
		{int64(123), "abc", true},
		{int64(123), int64(234), true},
		{int64(123), int64(123), false},
		{int64(123), int64(12), false},

		{"abc", false, false},
		{"abc", float64(1.23), false},
		{"abc", int64(123), false},
		{"def", "ghi", true},
		{"def", "def", false},
		{"def", "abc", false},
	}

	for _, c := range cases {
		if sql.Less(c.v1, c.v2) != c.less {
			t.Errorf("Less(%v, %v) got %t want %t", c.v1, c.v2, !c.less, c.less)
		}
	}
}
