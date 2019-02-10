package testutil_test

import (
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

type testStruct struct {
	N int
	S string
}

type xxxStruct struct {
	N       int
	S       string
	XXX_xxx bool
}

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
		{testStruct{1, "abc"}, testStruct{1, "abc"}, true},
		{testStruct{2, "abc"}, testStruct{1, "abc"}, false},
		{testStruct{1, "abc"}, testStruct{1, "def"}, false},
		{testStruct{1, "abc"}, &testStruct{1, "abc"}, false},
		{&testStruct{1, "abc"}, &testStruct{1, "abc"}, true},
		{&testStruct{2, "abc"}, &testStruct{1, "abc"}, false},
		{&testStruct{1, "abc"}, &testStruct{1, "def"}, false},
		{testStruct{1, "abc"}, xxxStruct{N: 1, S: "abc"}, false},
		{xxxStruct{N: 1, S: "abc"}, xxxStruct{N: 1, S: "abc"}, true},
		{xxxStruct{N: 1, S: "abc"}, xxxStruct{N: 1, S: "abc", XXX_xxx: true}, true},
		{xxxStruct{N: 2, S: "abc"}, xxxStruct{N: 1, S: "abc", XXX_xxx: true}, false},
	}

	for _, c := range cases {
		if testutil.DeepEqual(c.a, c.b) != c.ret {
			t.Errorf("DeepEqual(%v, %v) got %v want %v", c.a, c.b, !c.ret, c.ret)
		}
	}

	for _, c := range cases {
		var s string
		testutil.DeepEqual(c.a, c.b, &s)
		if c.ret {
			if s != "" {
				t.Errorf("DeepEqual(%v, %v, &s) succeeded; got %q for s; want \"\"", c.a, c.b, s)
			}
		} else {
			if s == "" {
				t.Errorf("DeepEqual(%v, %v, &s) failed; got \"\" for s", c.a, c.b)
			}
		}
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("DeepEqual(123, 123, &s1, &s2) did not panic")
		}
	}()
	var s1, s2 string
	testutil.DeepEqual(123, 123, &s1, &s2)
}
