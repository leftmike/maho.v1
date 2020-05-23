package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/engine/memrows"
	"github.com/leftmike/maho/engine/rowcols"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

func testEngine(t *testing.T, e engine.Engine) {
	cases := []struct {
		s string
		r string
	}{
		{`
select * from information_schema.tables
    where table_name != 'locks' and table_name != 'transactions'
    order by table_name
`,
			`   table_catalog         table_schema    table_name
   -------------         ------------    ----------
 1      'system' 'information_schema'     'columns'
 2      'system'            'virtual'      'config'
 3      'system'            'virtual'   'databases'
 4      'system'            'virtual' 'identifiers'
 5      'system' 'information_schema'    'schemata'
 6      'system' 'information_schema'      'tables'
(6 rows)
`},
		{"select table_schema, table_name, column_name from (show columns from identifiers) as c",
			`   table_schema    table_name column_name
   ------------    ---------- -----------
 1    'virtual' 'identifiers'      'name'
 2    'virtual' 'identifiers'        'id'
 3    'virtual' 'identifiers'  'reserved'
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
			`      SCHEMA
      ------
 1 'virtual'
(1 rows)
`},
		{"show schemas",
			`   catalog_name          schema_name
   ------------          -----------
 1     'system' 'information_schema'
 2     'system'            'virtual'
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

	for i, c := range cases {
		var b bytes.Buffer
		ses := &evaluate.Session{
			Engine:          e,
			DefaultDatabase: sql.SYSTEM,
			DefaultSchema:   sql.VIRTUAL,
		}
		replSQL(ses, parser.NewParser(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i)), &b)
		if b.String() != c.r {
			t.Errorf("parse(%q) got\n%s\nwant\n%s", c.s, b.String(), c.r)
		}
	}
}

func TestMain(t *testing.T) {
	e, err := basic.NewEngine("testdata")
	if err != nil {
		t.Fatal(err)
	}
	testEngine(t, e)

	e, err = memrows.NewEngine("testdata")
	if err != nil {
		t.Fatal(err)
	}
	testEngine(t, e)

	err = testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}
	e, err = rowcols.NewEngine("testdata")
	if err != nil {
		t.Fatal(err)
	}
	testEngine(t, e)
}
