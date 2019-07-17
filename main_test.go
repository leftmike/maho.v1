package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/engine/memrows"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

func TestMain(t *testing.T) {
	cases := []struct {
		s string
		r string
	}{
		{"select * from information_schema.tables order by table_name",
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
		{"select table_schema, table_name, column_name from (show columns from identifiers) as c",
			`   table_schema    table_name column_name
   ------------    ---------- -----------
 1     'public' 'identifiers'      'name'
 2     'public' 'identifiers'        'id'
 3     'public' 'identifiers'  'reserved'
(3 rows)
`},
		{"show database",
			`   DATABASE
   --------
 1 'system'
(1 rows)
`},
		{"show databases",
			`   database
   --------
 1 'system'
(1 rows)
`},
		{"show schema",
			`     SCHEMA
     ------
 1 'public'
(1 rows)
`},
		{"show schemas",
			`   catalog_name          schema_name
   ------------          -----------
 1     'system' 'information_schema'
 2     'system'             'public'
(2 rows)
`},
		{"select * from (show tables from information_schema) as c order by table_name",
			`   table_catalog         table_schema table_name
   -------------         ------------ ----------
 1      'system' 'information_schema'  'columns'
 2      'system' 'information_schema' 'schemata'
 3      'system' 'information_schema'   'tables'
(3 rows)
`},
	}

	e := memrows.NewEngine("testdata")

	for i, c := range cases {
		var b bytes.Buffer
		ses := &evaluate.Session{
			Engine:          e,
			DefaultDatabase: sql.SYSTEM,
			DefaultSchema:   sql.PUBLIC,
		}
		replSQL(ses, parser.NewParser(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i)), &b)
		if b.String() != c.r {
			t.Errorf("parse(%q) got\n%s\nwant\n%s", c.s, b.String(), c.r)
		}
	}
}
