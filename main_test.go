package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/parser"
)

func TestMain(t *testing.T) {
	cases := []struct {
		s string
		r string
	}{
		{"select * from system.db$tables order by [table]",
			`           table   id page_num      type
           -----   -- --------      ----
 1      'config' NULL     NULL 'virtual'
 2   'databases' NULL     NULL 'virtual'
 3  'db$columns' NULL     NULL 'virtual'
 4   'db$tables' NULL     NULL 'virtual'
 5     'engines' NULL     NULL 'virtual'
 6 'identifiers' NULL     NULL 'virtual'
`},
	}

	for i, c := range cases {
		var b bytes.Buffer
		replSQL(parser.NewParser(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i)), &b)
		if b.String() != c.r {
			t.Errorf("parse(%q) got\n%s\nwant\n%s", c.s, b.String(), c.r)
		}
	}
}
