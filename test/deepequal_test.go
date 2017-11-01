package test_test

import (
	"testing"

	"maho/sql"
	"maho/test"
)

func TestDeepEqual(t *testing.T) {
	cases := []struct {
		a, b interface{}
		ret  bool
	}{
		{1, 2, false},
		{"abc", "abc", true},
		{[]string{"abc", "def"}, []string{"abc", "def"}, true},
		{sql.ID("id"), sql.ID("id"), true},
		{sql.ID("id"), sql.ID("di"), false},
		{[]sql.Value{}, []sql.Value{}, true},
		{[][]sql.Value{}, [][]sql.Value{}, true},
	}

	for _, c := range cases {
		if test.DeepEqual(c.a, c.b) != c.ret {
			t.Errorf("DeepEqual(%v, %v) got %v want %v", c.a, c.b, !c.ret, c.ret)
		}
	}
}
