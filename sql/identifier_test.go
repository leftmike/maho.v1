package sql

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"strings"
	"testing"
)

func TestID(t *testing.T) {
	equal := []struct{ s1, s2 string }{
		{"abc", "abc"},
		{"Abc", "abc"},
		{"abC", "abc"},
		{"ABC", "abc"},
		{"create", "create"},
		{"create", "CREATE"},
	}

	for _, c := range equal {
		if UnquotedID(c.s1) != UnquotedID(c.s2) {
			t.Errorf("UnquotedID(%q) != UnquotedID(%q)", c.s1, c.s2)
		}
	}

	notEqual := []struct{ s1, s2 string }{
		{"abc", "abcd"},
		{"abcd", "abc"},
		{"abc", "ABCD"},
		{"ABCD", "abc"},
	}

	for _, c := range notEqual {
		if ID(c.s1) == ID(c.s2) {
			t.Errorf("ID(%q) == ID(%q)", c.s1, c.s2)
		}
	}

	if ID(strings.Repeat("x", MaxIdentifier+1)).String() != strings.Repeat("x", MaxIdentifier) {
		t.Errorf(`ID(strings.Repeat("x", MaxIdentifier+1)).String() !=
strings.Repeat("x", MaxIdentifier)`)
	}

	if QuotedID(strings.Repeat("y", MaxIdentifier+2)).String() !=
		strings.Repeat("y", MaxIdentifier) {
		t.Errorf(`QuotedID(strings.Repeat("y", MaxIdentifier+2)).String() !=
strings.Repeat("y", MaxIdentifier)`)
	}
}

func TestQuotedID(t *testing.T) {
	equal := []struct{ s1, s2 string }{
		{"abc", "abc"},
		{"Abc", "abc"},
		{"abC", "abc"},
		{"ABC", "abc"},
	}

	for _, c := range equal {
		if ID(c.s1) != QuotedID(c.s2) {
			t.Errorf("ID(%q) != QuotedID(%q)", c.s1, c.s2)
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
		if UnquotedID(c.s1) == QuotedID(c.s2) {
			t.Errorf("UnquotedID(%q) == QuotedID(%q)", c.s1, c.s2)
		}
	}
}

func TestString(t *testing.T) {
	id := ID("abc")
	ids := []string{"abc", "defg", "hijk", "lmnop", "qrstuv"}
	for _, s := range ids {
		ID(s)
	}
	if id.String() != ID("abc").String() {
		t.Errorf("ID(%q).String() != ID(\"abc\").String()", id.String())
	}
}

func TestIsReserved(t *testing.T) {
	ids := []string{"abc", "defg", "hijk", "lmnop", "qrstuv", "int", "INT"}
	for _, s := range ids {
		if UnquotedID(s).IsReserved() {
			t.Errorf("ID(%q).IsReserved() got true want false", s)
		}
	}

	kws := []string{"create", "CREATE", "update", "select", "SELECT"}
	for _, s := range kws {
		if !UnquotedID(s).IsReserved() {
			t.Errorf("ID(%q).IsReserved() got false want true", s)
		}
	}
}

func TestKeywords(t *testing.T) {
	for s, n := range knownKeywords {
		if s != strings.ToUpper(s) {
			t.Errorf("%q != strings.ToUpper(%q)", s, s)
		}

		if UnquotedID(s) != n.id {
			t.Errorf("ID(%q) != knownKeywords[%q].id", s, s)
		}
		if n.id.IsReserved() != n.reserved {
			t.Errorf("knownKeywords[%q].id.IsReserved() != knownKeywords[%q].reserved", s, s)
		}
	}
}

func TestKnownIdentifiers(t *testing.T) {
	for s, id := range knownIdentifiers {
		if UnquotedID(s) != id {
			t.Errorf("ID(%q) != knownIdentifiers[%q]", s, s)
		}
		if id.IsReserved() {
			t.Errorf("knownIdentifiers[%q].IsReserved() got true want false", s)
		}
	}
}

func testGob(t *testing.T, in interface{}, out interface{}) {
	t.Helper()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(in)
	if err != nil {
		t.Errorf("Encode(%#v) failed with %s", in, err)
	}
	dec := gob.NewDecoder(&buf)
	err = dec.Decode(out)
	if err != nil {
		t.Errorf("Decode(%#v) failed with %s", out, err)
	}

	if !reflect.DeepEqual(in, out) {
		t.Errorf("Encode -> Decode: got %v want %v", out, in)
	}
}

func TestGob(t *testing.T) {

	idi := ID("testing")
	var ido Identifier
	testGob(t, &idi, &ido)

	idi = PUBLIC
	testGob(t, &idi, &ido)

	idi = CREATE
	testGob(t, &idi, &ido)

	si := []Identifier{ID("testing"), CREATE, TABLE, SYSTEM}
	var so []Identifier
	testGob(t, &si, &so)
}
