package scanner_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	. "github.com/leftmike/maho/parser/scanner"
	"github.com/leftmike/maho/parser/token"
	"github.com/leftmike/maho/sql"
)

func TestScan(t *testing.T) {
	cases := []struct {
		s string
		r rune
	}{
		{"", token.EOF},
		{";", token.EndOfStatement},
		{"abc", token.Identifier},
		{"create", token.Reserved},
		{"'create'", token.String},
		{"`create`", token.Identifier},
		{"[create]", token.Identifier},
		{"\"create\"", token.Identifier},
		{"'isn\\'t go fun?'", token.String},
		{"12345", token.Integer},
		{"1234.5678", token.Float},
		{", ", token.Comma},
		{".id", token.Dot},
		{"(123", token.LParen},
		{")+", token.RParen},
		{"-abc", token.Minus},
		{"+abc", token.Plus},
		{"*(abc)", token.Star},
		{"/12", token.Slash},
		{"%", token.Percent},
		{"=123", token.Equal},
		{"<123", token.Less},
		{">123", token.Greater},
		{"&123", token.Ampersand},
		{"|123", token.Bar},
		{"||", token.BarBar},
		{"<<", token.LessLess},
		{"<=", token.LessEqual},
		{"<>", token.LessGreater},
		{">>", token.GreaterGreater},
		{">=", token.GreaterEqual},
		{"==", token.EqualEqual},
		{"!=", token.BangEqual},
		{"!*", token.Error},
		{"**", token.Error},
		{">%", token.Error},
		{">-123", token.Greater},
		{"=>", token.Error},
	}

	for i, c := range cases {
		var s Scanner
		s.Init(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i))
		var sctx ScanCtx
		s.Scan(&sctx)
		if sctx.Token != c.r {
			t.Errorf("Scan(%q) got %d want %d", c.s, sctx.Token, c.r)
		}
	}

	string_cases := []struct {
		s   string
		ret string
	}{
		{"'abc'", "abc"},
		{"'abc' 123", "abc"},
		{"'abc' 'def'", "abc"},
		{"'abc'\n'def'", "abcdef"},
		{"'abc'\r'def'", "abcdef"},
		{"'abc'\n 'def'", "abcdef"},
		{"'abc' \r\n  \r  \n 'def'", "abcdef"},
		{"'abc' \r\n  \r  \n 123", "abc"},
		{"'abc' \r\n  \r  \n", "abc"},
		{"'abc''def' 123", "abc'def"},
		{"e'abc'\n 'def'", "abcdef"},
		{"E'abc'\n 'def'", "abcdef"},
		{`e'\000abc'`, "\000abc"},
		{`e'\000\141bc'`, "\000abc"},
		{`e'\141\x62\u0063\U00000064e'`, "abcde"},
	}

	for i, c := range string_cases {
		var s Scanner
		s.Init(strings.NewReader(c.s), fmt.Sprintf("strings[%d]", i))
		var sctx ScanCtx
		s.Scan(&sctx)
		if sctx.Token != token.String {
			t.Errorf("Scan(%q) got %d want String", c.s, sctx.Token)
		}
		if sctx.String != c.ret {
			t.Errorf("Scan(%q).String got %s want %s", c.s, sctx.String, c.ret)
		}
	}

	bytes_cases := []struct {
		s string
		b []byte
	}{
		{`x'6263646566'`, []byte{0x62, 0x63, 0x64, 0x65, 0x66}},
		{`x''`, []byte{}},
	}

	for i, c := range bytes_cases {
		var s Scanner
		s.Init(strings.NewReader(c.s), fmt.Sprintf("bytes[%d]", i))
		var sctx ScanCtx
		s.Scan(&sctx)
		if sctx.Token != token.Bytes {
			t.Errorf("Scan(%q) got %d want Bytes", c.s, sctx.Token)
		}
		if bytes.Compare(sctx.Bytes, c.b) != 0 {
			t.Errorf("Scan(%q).Bytes got %v want %v", c.s, sctx.Bytes, c.b)
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
		var sctx ScanCtx
		s.Scan(&sctx)
		if sctx.Token != token.Integer {
			t.Errorf("Scan(%q) got %d want Integer", n.s, sctx.Token)
		}
		if sctx.Integer != n.n {
			t.Errorf("Scan(%q).Integer got %d want %d", n.s, sctx.Integer, n.n)
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
		var sctx ScanCtx
		s.Scan(&sctx)
		if sctx.Token != token.Float {
			t.Errorf("Scan(%q) got %d want Float", n.s, sctx.Token)
		}
		if sctx.Float != n.n {
			t.Errorf("Scan(%q).Float got %f want %f", n.s, sctx.Float, n.n)
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
			{ret: token.Reserved, id: sql.CREATE},
			{ret: token.Identifier, s: "create"},
			{ret: token.String, s: "create"},
			{ret: token.Identifier, s: "abcd"},
			{ret: token.EOF},
		}

		var s Scanner
		s.Init(strings.NewReader(src), "src")
		for i, e := range expected {
			var sctx ScanCtx
			s.Scan(&sctx)
			if sctx.Token != e.ret {
				t.Errorf("Scan(%q)[%d] got %d want %d", src, i, sctx.Token, e.ret)
			}
			switch e.ret {
			case token.Identifier:
				if sctx.Identifier != sql.QuotedID(e.s) {
					t.Errorf("%d Scan(%q) != sql.QuotedID(%q)", i, src, e.s)
				}
			case token.Reserved:
				if sctx.Identifier != e.id {
					t.Errorf("%d Scan(%q).Identifier != %d", i, src, e.id)
				}
			case token.String:
				if sctx.String != e.s {
					t.Errorf("%d Scan(%q).String != %q", i, src, e.s)
				}
			}
		}
	}
}
