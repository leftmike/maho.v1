package test_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/flags"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage/basic"
)

func TestValuesSimple(t *testing.T) {
	cases := []struct {
		sql  string
		fail bool
		rows [][]sql.Value
	}{
		{
			sql: "values (true, 'abcd', 123.456, 789)",
			rows: [][]sql.Value{{sql.BoolValue(true), sql.StringValue("abcd"),
				sql.Float64Value(123.456), sql.Int64Value(789)}},
		},
		{
			sql: "values (1 + 2, 3, 4 - 5), (12, 34, 56.7 * 8)",
			rows: [][]sql.Value{
				{sql.Int64Value(3), sql.Int64Value(3), sql.Int64Value(-1)},
				{sql.Int64Value(12), sql.Int64Value(34), sql.Float64Value(453.6)},
			},
		},
	}

	st, err := basic.NewStore("testdata")
	if err != nil {
		t.Fatal(err)
	}
	e := engine.NewEngine(st, flags.Default())

	err = e.CreateDatabase(sql.ID("core_test"), nil)
	if err != nil {
		// If the test is run multiple times, then the database will already exist.
	}

	ses := evaluate.NewSession(e, sql.ID("core_test"), sql.PUBLIC)
	ctx := context.Background()

	for i, c := range cases {
		p := parser.NewParser(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed with %s", c.sql, err)
			continue
		}
		tx := e.Begin(0)

		plan, err := stmt.Plan(ctx, ses, tx, nil)
		if c.fail {
			if err == nil {
				t.Errorf("Plan(%q) did not fail", c.sql)
			}
			continue
		}
		if err != nil {
			t.Errorf("Plan(%q) failed with %s", c.sql, err)
			continue
		}
		rowsPlan, ok := plan.(evaluate.RowsPlan)
		if !ok {
			t.Errorf("(%v).Plan() did not return Rows", c.sql)
			continue
		}
		rows, err := rowsPlan.Rows(ctx, tx, nil)
		if err != nil {
			t.Errorf("(%v).Rows() failed with %s", c.sql, err)
			continue
		}
		dest := make([]sql.Value, rows.NumColumns())
		for i, r := range c.rows {
			if rows.Next(ctx, dest) != nil {
				t.Errorf("Plan(%q).Rows() got %d rows; want %d rows", c.sql, i, len(c.rows))
				break
			}
			if !reflect.DeepEqual(dest, r) {
				t.Errorf("Plan(%q).Rows()[%d] got %q want %q", c.sql, i, dest, r)
				break
			}
		}

		err = tx.Commit(ctx)
		if err != nil {
			t.Error(err)
		}
	}
}
