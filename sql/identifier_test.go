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
			t.Errorf("Id(%q) != Id(%q)", c.s1, c.s2)
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
			t.Errorf("Id(%q) == Id(%q)", c.s1, c.s2)
		}
	}

	if Id(strings.Repeat("x", MaxIdentifier+1)).String() != strings.Repeat("x", MaxIdentifier) {
		t.Errorf(`Id(strings.Repeat("x", MaxIdentifier+1)).String() !=
strings.Repeat("x", MaxIdentifier)`)
	}

	if QuotedId(strings.Repeat("y", MaxIdentifier+2)).String() !=
		strings.Repeat("y", MaxIdentifier) {
		t.Errorf(`QuotedId(strings.Repeat("y", MaxIdentifier+2)).String() !=
strings.Repeat("y", MaxIdentifier)`)
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
			t.Errorf("Id(%q) != QuotedId(%q)", c.s1, c.s2)
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
			t.Errorf("Id(%q) == QuotedId(%q)", c.s1, c.s2)
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
		t.Errorf("Id(%q).String() != Id(\"abc\").String()", id.String())
	}
}

func TestIsReserved(t *testing.T) {
	ids := []string{"abc", "defg", "hijk", "lmnop", "qrstuv", "int", "INT"}
	for _, s := range ids {
		if Id(s).IsReserved() {
			t.Errorf("Id(%q).IsReserved() got true want false", s)
		}
	}

	kws := []string{"create", "CREATE", "update", "select", "SELECT"}
	for _, s := range kws {
		if !Id(s).IsReserved() {
			t.Errorf("Id(%q).IsReserved() got false want true", s)
		}
	}
}

func TestKeywords(t *testing.T) {
	for s, n := range knownKeywords {
		if s != strings.ToUpper(s) {
			t.Errorf("%q != strings.ToUpper(%q)", s, s)
		}

		if Id(s) != n.id {
			t.Errorf("Id(%q) != knownKeywords[%q].id", s, s)
		}
		if n.id.IsReserved() != n.reserved {
			t.Errorf("knownKeywords[%q].id.IsReserved() != knownKeywords[%q].reserved", s, s)
		}
	}
}

func TestKnownIdentifiers(t *testing.T) {
	for s, id := range knownIdentifiers {
		if Id(s) != id {
			t.Errorf("Id(%q) != knownIdentifiers[%q]", s, s)
		}
		if id.IsReserved() {
			t.Errorf("knownIdentifiers[%q].IsReserved() got true want false", s)
		}
	}
}
