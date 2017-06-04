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
		{"12345", Number},
	}

	for i, c := range cases {
		var s Scanner
		s.Init(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i))
		if s.Scan() != c.r {
			t.Errorf("scan: \"%s\": expected: %d", c.s, c.r)
		}
	}

	numbers := []struct {
		s string
		n int
	}{
		{"12345", 12345},
		{"999", 999},
		{"999 ", 999},
		{"999zzz", 999},
		{"-123", -123},
		{"+123", 123},
	}

	for i, n := range numbers {
		var s Scanner
		s.Init(strings.NewReader(n.s), fmt.Sprintf("numbers[%d]", i))
		if s.Scan() != Number {
			t.Errorf("scan: \"%s\": not a number", n.s)
		}
		if s.Number != n.n {
			t.Errorf("scan: \"%s\": %d != %d", n.s, s.Number, n.n)
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
			if s.Scan() != e.ret {
				t.Errorf("scan: \"%s\": expected[%d]: %d", src, i, e.ret)
			}
			switch e.ret {
			case Identifier:
				if s.Identifier != sql.QuotedId(e.s) {
					t.Errorf("scan: \"%s\": wrong identifier: %d != %d", src, s.Identifier,
						sql.Id(e.s))
				}
			case Reserved:
				if s.Identifier != e.id {
					t.Errorf("scan: \"%s\": wrong keyword: %d != %d", src, s.Identifier, e.id)
				}
			case String:
				if s.String != e.s {
					t.Errorf("scan: \"%s\": wrong string: \"%s\" != \"%s\"", src, s.String, e.s)
				}
			}
		}
	}
}
