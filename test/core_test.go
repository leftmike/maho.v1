package test_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/plan"
	"github.com/leftmike/maho/sql"
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

	startEngine(t)
	for i, c := range cases {
		p := parser.NewParser(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed with %s", c.sql, err)
			continue
		}
		tx, err := engine.Begin("basic", sql.ID("test"))
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		ret, err := stmt.Plan(ctx, tx)
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
		rows, ok := ret.(plan.Rows)
		if !ok {
			t.Errorf("Plan(%q).(plan.Rows) failed", c.sql)
			continue
		}
		dest := make([]sql.Value, len(rows.Columns()))
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
	}
}
