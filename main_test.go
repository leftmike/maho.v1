package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/parser"
)

func TestMain(t *testing.T) {
	err := start()
	if err != nil {
		t.Errorf("start() failed with %s", err)
		return
	}
	cases := []struct {
		s string
		r string
	}{
		{"select * from system.db$tables order by [table]",
			`           table   id page_num      type
           -----   -- --------      ----
 1   'databases' NULL     NULL 'virtual'
 2  'db$columns' NULL     NULL 'virtual'
 3   'db$tables' NULL     NULL 'virtual'
 4 'identifiers' NULL     NULL 'virtual'
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
