package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/engine/memrows"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/parser"
)

func TestMain(t *testing.T) {
	cases := []struct {
		s string
		r string
	}{
		{"select * from system.information_schema.tables order by table_name",
			`   table_catalog         table_schema     table_name
   -------------         ------------     ----------
 1      'system' 'information_schema'      'columns'
 2      'system'             'public'       'config'
 3      'system'             'public'    'databases'
 4      'system'             'public'  'identifiers'
 5      'system'             'public'        'locks'
 6      'system' 'information_schema'     'schemata'
 7      'system' 'information_schema'       'tables'
 8      'system'             'public' 'transactions'
(8 rows)
`},
	}

	e := memrows.NewEngine("testdata")

	for i, c := range cases {
		var b bytes.Buffer
		ses := &evaluate.Session{
			Engine: e,
		}
		replSQL(ses, parser.NewParser(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i)), &b)
		if b.String() != c.r {
			t.Errorf("parse(%q) got\n%s\nwant\n%s", c.s, b.String(), c.r)
		}
	}
}
