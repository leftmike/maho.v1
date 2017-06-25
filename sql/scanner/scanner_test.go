package scanner_test

import (
	"fmt"
	"maho/sql"
	. "maho/sql/scanner"
	"strings"
	"testing"
)

func TestScan(t *testing.T) {
	cases := []struct {
		s string
		r rune
	}{
		{"", EOF},
		{"abc", Identifier},
		{"create", Reserved},
		{"'create'", String},
		{"`create`", Identifier},
		{"[create]", Identifier},
		{"\"create\"", Identifier},
		{"'isn\\'t go fun?'", String},
		{"12345", Integer},
		{"1234.5678", Double},
	}

	for i, c := range cases {
		var s Scanner
		s.Init(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i))
		r := s.Scan()
		if r != c.r {
			t.Errorf("Scan(%q) got %d want %d", c.s, r, c.r)
		}
	}

	integers := []struct {
		s string
		n int64
	}{
		{"12345", 12345},
		{"999", 999},
		{"999 ", 999},
		{"999zzz", 999},
		{"-123", -123},
		{"+123", 123},
	}

	for i, n := range integers {
		var s Scanner
		s.Init(strings.NewReader(n.s), fmt.Sprintf("integers[%d]", i))
		r := s.Scan()
		if r != Integer {
			t.Errorf("Scan(%q) got %d want Integer", n.s, r)
		}
		if s.Integer != n.n {
			t.Errorf("Scan(%q).Integer got %d want %d", n.s, s.Integer, n.n)
		}
	}

	doubles := []struct {
		s string
		n float64
	}{
		{"123.456", 123.456},
		{"999.", 999.0},
		{"99.9 ", 99.9},
		{"9.99zzz", 9.99},
		{"-12.3", -12.3},
		{"+1.23", 1.23},
	}

	for i, n := range doubles {
		var s Scanner
		s.Init(strings.NewReader(n.s), fmt.Sprintf("doubles[%d]", i))
		r := s.Scan()
		if r != Double {
			t.Errorf("Scan(%q) got %d want Double", n.s, r)
		}
		if s.Double != n.n {
			t.Errorf("Scan(%q).Double got %f want %f", n.s, s.Double, n.n)
		}
	}

	{
		src := `
-- start with a comment
create -- reserved keyword
"create" /* identifier */
'create' /* string

*/
abcd -- identifier
`
		expected := []struct {
			ret rune
			id  sql.Identifier
			s   string
		}{
			{ret: Reserved, id: sql.CREATE},
			{ret: Identifier, s: "create"},
			{ret: String, s: "create"},
			{ret: Identifier, s: "abcd"},
			{ret: EOF},
		}

		var s Scanner
		s.Init(strings.NewReader(src), "src")
		for i, e := range expected {
			r := s.Scan()
			if r != e.ret {
				t.Errorf("Scan(%q)[%d] got %d want %d", src, i, r, e.ret)
			}
			switch e.ret {
			case Identifier:
				if s.Identifier != sql.QuotedId(e.s) {
					t.Errorf("%d Scan(%q) != sql.QuotedId(%q)", i, src, e.s)
				}
			case Reserved:
				if s.Identifier != e.id {
					t.Errorf("%d Scan(%q).Identifier != %d", i, src, e.id)
				}
			case String:
				if s.String != e.s {
					t.Errorf("%d Scan(%q).String != %q", i, src, e.s)
				}
			}
		}
	}
}
