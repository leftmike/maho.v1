package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/flags"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/repl"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/basic"
	"github.com/leftmike/maho/storage/rowcols"
	"github.com/leftmike/maho/testutil"
)

type testCase struct {
	s, r string
}

var (
	cases = []testCase{
		{`
select * from metadata.tables
    where table_name != 'locks' and table_name != 'transactions' and schema_name != 'private'
    order by table_name
`,
			`  database_name schema_name table_name  
  ------------- ----------- ----------  
1 system        metadata    columns     
2 system        metadata    constraints 
3 system        info        databases   
4 system        info        identifiers 
5 system        metadata    schemas     
6 system        metadata    tables      
(6 rows)
`},
		{"select schema_name, table_name, column_name from (show columns from identifiers) as c",
			`  schema_name table_name  column_name 
  ----------- ----------  ----------- 
1 info        identifiers name        
2 info        identifiers id          
3 info        identifiers reserved    
(3 rows)
`},
		{"show database",
			`  DATABASE 
  -------- 
1 system   
(1 rows)
`},
		{"show databases",
			`  database 
  -------- 
1 system   
(1 rows)
`},
		{"show schema",
			`  SCHEMA 
  ------ 
1 info   
(1 rows)
`},
		{"select * from (show tables from metadata) as c order by table_name",
			`  database_name schema_name table_name  
  ------------- ----------- ----------  
1 system        metadata    columns     
2 system        metadata    constraints 
3 system        metadata    schemas     
4 system        metadata    tables      
(4 rows)
`},
		{"show schemas",
			`  database_name schema_name 
  ------------- ----------- 
1 system        private     
2 system        metadata    
3 system        info        
(3 rows)
`},
		{"select * from metadata.tables order by table_name, schema_name",
			`   database_name schema_name table_name  
   ------------- ----------- ----------  
1  system        metadata    columns     
2  system        metadata    constraints 
3  system        info        databases   
4  system        private     databases   
5  system        info        identifiers 
6  system        metadata    schemas     
7  system        private     schemas     
8  system        private     sequences   
9  system        metadata    tables      
10 system        private     tables      
(10 rows)
`},
		{`select * from metadata.constraints
where table_name = 'tables' and schema_name = 'metadata'
order by table_name, schema_name, constraint_name`,
			`  database_name schema_name table_name constraint_name constraint_type details              
  ------------- ----------- ---------- --------------- --------------- -------              
1 system        metadata    tables     NULL            NOT NULL        column database_name 
2 system        metadata    tables     NULL            NOT NULL        column schema_name   
3 system        metadata    tables     NULL            NOT NULL        column table_name    
(3 rows)
`},
	}
)

func testStore(t *testing.T, st *storage.Store, cases []testCase) {
	e := engine.NewEngine(st, flags.Default())

	for i, c := range cases {
		ses := evaluate.NewSession(e, sql.SYSTEM, sql.INFO)

		var b bytes.Buffer
		repl.ReplSQL(ses, parser.NewParser(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i)),
			&b)
		if b.String() != c.r {
			t.Errorf("parse(%q) got\n%s\nwant\n%s", c.s, b.String(), c.r)
		}
	}
}

func TestMain(t *testing.T) {
	st, err := basic.NewStore("testdata")
	if err != nil {
		t.Fatal(err)
	}
	testStore(t, st, cases)

	err = testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}
	st, err = rowcols.NewStore("testdata")
	if err != nil {
		t.Fatal(err)
	}
	testStore(t, st, cases)
}
