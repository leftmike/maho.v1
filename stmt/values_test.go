package stmt_test

import (
	"fmt"
	"maho/db"
	"maho/engine"
	"maho/parser"
	"maho/sql"
	"maho/store"
	"reflect"
	"strings"
	"testing"
)

func TestValues(t *testing.T) {
	cases := []struct {
		sql  string
		fail bool
		rows [][]sql.Value
	}{
		{
			sql:  "values (true, 'abcd', 123.456, 789)",
			rows: [][]sql.Value{{true, "abcd", 123.456, int64(789)}},
		},
		{
			sql: "values (1 + 2, 3, 4 - 5), (12, 34, 56.7 * 8)",
			rows: [][]sql.Value{
				{int64(3), int64(3), int64(-1)},
				{int64(12), int64(34), 453.6},
			},
		},
	}

	dbase, err := store.Open("test", "test_insert")
	if err != nil {
		t.Fatal(err)
	}
	e, err := engine.Start(dbase)
	if err != nil {
		t.Fatal(err)
	}

	for i, c := range cases {
		p := parser.NewParser(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed with %s", c.sql, err)
			continue
		}
		ret, err := stmt.Execute(e)
		if c.fail {
			if err == nil {
				t.Errorf("Execute(%q) did not fail", c.sql)
			}
			continue
		}
		if err != nil {
			t.Errorf("Execute(%q) failed with %s", c.sql, err)
			continue
		}
		rows, ok := ret.(db.Rows)
		if !ok {
			t.Errorf("Execute(%q).(db.Rows) failed", c.sql)
			continue
		}
		dest := make([]sql.Value, len(rows.Columns()))
		for i, r := range c.rows {
			if rows.Next(dest) != nil {
				t.Errorf("Execute(%q) got %d rows; want %d rows", c.sql, i, len(c.rows))
				break
			}
			if !reflect.DeepEqual(dest, r) {
				t.Errorf("Execute(%q)[%d] got %q want %q", c.sql, i, dest, r)
				break
			}
		}
	}
}
