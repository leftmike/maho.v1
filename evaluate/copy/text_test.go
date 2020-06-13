package copy

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/sql"
)

func TestReadTextColumn(t *testing.T) {
	cases := []struct {
		s    string
		d    rune
		last bool
		null bool
		r    string
		fail bool
	}{
		{s: "abcd|defg", d: '|', r: "abcd"},
		{s: `ab\|cd|defg`, d: '|', r: "ab|cd"},
		{s: "abcd\ndefg", d: '|', last: true, r: "abcd"},
		{s: "abcd|defg", d: '|', last: true, fail: true},
		{s: "abcd$defg", d: '$', r: "abcd"},
		{s: `ab\$cd$defg`, d: '$', r: "ab$cd"},
		{s: "abcd\ndefg", d: '$', last: true, r: "abcd"},
		{s: "abcd$defg", d: '$', last: true, fail: true},
		{s: `\b\f\n\r\t\v|`, d: '|', r: "\b\f\n\r\t\v"},
		{s: `\N|`, d: '|', null: true},
		{s: "\\N\n", d: '|', last: true, null: true},
		{s: `abc\Ndef|`, d: '|', null: true, r: "abcdef"},
	}

	for _, c := range cases {
		rdr := NewReader("test", strings.NewReader(c.s))
		r, _, err := readTextColumn(rdr, c.d, c.last)
		if err != nil {
			if !c.fail {
				t.Errorf("readTextColumn(%s) failed with %s", c.s, err)
			}
		} else if c.fail {
			t.Errorf("readTextColumn(%s) did not fail", c.s)
		}
		if c.r != r {
			t.Errorf("readTextColumn(%s) got %s want %s", c.s, r, c.r)
		}
	}
}

func testCopyFromText(t *testing.T, numCols int, delim rune, rdr *Reader, results [][]sql.Value) {
	err := CopyFromText(rdr, numCols, delim,
		func(vals []sql.Value) error {
			if len(results) == 0 {
				return errors.New("not enough results")
			}
			if len(vals) != len(results[0]) {
				return fmt.Errorf("got %d values, want %d", len(vals), len(results[0]))
			}
			for vdx := range vals {
				if vals[vdx] != results[0][vdx] {
					return fmt.Errorf("got %v want %v", vals[vdx], results[0][vdx])
				}
			}
			results = results[1:]
			return nil
		})
	if err != nil {
		t.Errorf("CopyFromText() failed %s", err)
	} else if len(results) != 0 {
		t.Errorf("CopyFromText() not enough calls to function; %d remaining", len(results))
	}
}

func TestCopyFromText(t *testing.T) {
	testCopyFromText(t, 3, '|',
		NewReader("test", strings.NewReader(
			`123|456|789
abc|\N|def
\N|xyz|\N
\N|\N|\N
`)),
		[][]sql.Value{
			{sql.StringValue("123"), sql.StringValue("456"), sql.StringValue("789")},
			{sql.StringValue("abc"), nil, sql.StringValue("def")},
			{nil, sql.StringValue("xyz"), nil},
			{nil, nil, nil},
		})

	err := CopyFromText(NewReader("test", strings.NewReader(
		`123 \N 456|789
`)),
		2, '|',
		func(vals []sql.Value) error {
			return errors.New("function should not be called")
		})
	if err == nil {
		t.Errorf("CopyFromText() did not fail")
	}
}
