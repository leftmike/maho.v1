package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/engine"
)

func TestMain(t *testing.T) {
	cases := []struct {
		s string
		r string
	}{
		{"select * from system.db$tables order by [table]",
			`           table      type
           -----      ----
 1      'config' 'virtual'
 2   'databases' 'virtual'
 3  'db$columns' 'virtual'
 4   'db$tables' 'virtual'
 5     'engines' 'virtual'
 6 'identifiers' 'virtual'
 7       'locks' 'virtual'
(7 rows)
`},
	}

	mgr := engine.NewManager("testdata", map[string]engine.Engine{})

	for i, c := range cases {
		var b bytes.Buffer
		replSQL(mgr, strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i), &b, "")
		if b.String() != c.r {
			t.Errorf("parse(%q) got\n%s\nwant\n%s", c.s, b.String(), c.r)
		}
	}
}
