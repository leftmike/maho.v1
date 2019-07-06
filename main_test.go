package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/parser"
)

func TestMain(t *testing.T) {
	cases := []struct {
		s string
		r string
	}{
		{"select * from system.db$tables order by [table]",
			`            table      type
            -----      ----
 1       'config' 'virtual'
 2    'databases' 'virtual'
 3   'db$columns' 'virtual'
 4    'db$tables' 'virtual'
 5  'identifiers' 'virtual'
 6        'locks' 'virtual'
 7 'transactions' 'virtual'
(7 rows)
`},
	}

	mgr := engine.NewManager("testdata", &basic.Engine{})

	for i, c := range cases {
		var b bytes.Buffer
		ses := &evaluate.Session{
			Manager: mgr,
		}
		replSQL(ses, parser.NewParser(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i)), &b)
		if b.String() != c.r {
			t.Errorf("parse(%q) got\n%s\nwant\n%s", c.s, b.String(), c.r)
		}
	}
}
