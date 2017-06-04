package sql

import (
	"strings"
	"testing"
)

func TestId(t *testing.T) {
	equal := []struct{ s1, s2 string }{
		{"abc", "abc"},
		{"Abc", "abc"},
		{"abC", "abc"},
		{"ABC", "abc"},
		{"create", "create"},
		{"create", "CREATE"},
	}

	for _, c := range equal {
		if Id(c.s1) != Id(c.s2) {
			t.Errorf("id: \"%s\" != \"%s\"", c.s1, c.s2)
		}
	}

	notEqual := []struct{ s1, s2 string }{
		{"abc", "abcd"},
		{"abcd", "abc"},
		{"abc", "ABCD"},
		{"ABCD", "abc"},
	}

	for _, c := range notEqual {
		if Id(c.s1) == Id(c.s2) {
			t.Errorf("id: \"%s\" == \"%s\"", c.s1, c.s2)
		}
	}
}

func TestQuotedId(t *testing.T) {
	equal := []struct{ s1, s2 string }{
		{"abc", "abc"},
		{"Abc", "abc"},
		{"abC", "abc"},
		{"ABC", "abc"},
	}

	for _, c := range equal {
		if Id(c.s1) != QuotedId(c.s2) {
			t.Errorf("id: \"%s\" != \"%s\"", c.s1, c.s2)
		}
	}

	notEqual := []struct{ s1, s2 string }{
		{"abc", "abcd"},
		{"abcd", "abc"},
		{"abc", "ABCD"},
		{"ABCD", "abc"},
		{"abc", "Abc"},
		{"abc", "abC"},
		{"abc", "ABC"},
		{"create", "create"},
		{"create", "CREATE"},
	}

	for _, c := range notEqual {
		if Id(c.s1) == QuotedId(c.s2) {
			t.Errorf("id: \"%s\" == \"%s\"", c.s1, c.s2)
		}
	}
}

func TestString(t *testing.T) {
	id := Id("abc")
	ids := []string{"abc", "defg", "hijk", "lmnop", "qrstuv"}
	for _, s := range ids {
		Id(s)
	}
	if id.String() != Id("abc").String() {
		t.Errorf("identifier.String: \"%s\" != \"%s\"", id.String(), Id("abc").String())
	}
}

func TestIsReserved(t *testing.T) {
	ids := []string{"abc", "defg", "hijk", "lmnop", "qrstuv", "int", "INT"}
	for _, s := range ids {
		if Id(s).IsReserved() {
			t.Errorf("is reserved: \"%s\"", s)
		}
	}

	kws := []string{"create", "CREATE", "update", "select", "SELECT"}
	for _, s := range kws {
		if !Id(s).IsReserved() {
			t.Errorf("is reserved: \"%s\"", s)
		}
	}
}

func TestKeywords(t *testing.T) {
	for s, n := range knownKeywords {
		if s != strings.ToUpper(s) {
			t.Errorf("keywords: \"%s\" must be upper-case", s)
		}

		if Id(s) != n.id {
			t.Errorf("keywords: \"%s\": %d %d\n", s, n.id, Id(s))
		}
		if n.id.IsReserved() != n.reserved {
			t.Errorf("keywords: \"%s\" is not a reserved", s)
		}
	}
}

func TestKnownIdentifiers(t *testing.T) {
	for s, id := range knownIdentifiers {
		if Id(s) != id {
			t.Errorf("known identifiers: \"%s\": %d\n", s, id)
		}
		if id.IsReserved() {
			t.Errorf("known identifiers: \"%s\" is a reserved", s)
		}
	}
}
