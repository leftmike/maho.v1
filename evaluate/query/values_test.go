package query_test

import (
	"reflect"
	"testing"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/evaluate/query"
	"github.com/leftmike/maho/evaluate/test"
	"github.com/leftmike/maho/sql"
)

func TestValues(t *testing.T) {
	cases := []struct {
		values query.Values
		s      string
		cols   []sql.Identifier
		rows   [][]sql.Value
	}{
		{
			values: query.Values{
				Expressions: [][]expr.Expr{
					{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True(), expr.Nil()},
				},
			},
			s: "VALUES (1, 'abc', true, NULL)",
			cols: []sql.Identifier{sql.ID("column1"), sql.ID("column2"), sql.ID("column3"),
				sql.ID("column4")},
			rows: [][]sql.Value{
				{sql.Int64Value(1), sql.StringValue("abc"), sql.BoolValue(true), nil},
			},
		},
		{
			values: query.Values{
				Expressions: [][]expr.Expr{
					{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True()},
					{expr.Int64Literal(2), expr.StringLiteral("def"), expr.False()},
					{expr.Int64Literal(3), expr.StringLiteral("ghi"), expr.True()},
					{expr.Int64Literal(4), expr.StringLiteral("jkl"), expr.False()},
				},
			},
			s:    "VALUES (1, 'abc', true), (2, 'def', false), (3, 'ghi', true), (4, 'jkl', false)",
			cols: []sql.Identifier{sql.ID("column1"), sql.ID("column2"), sql.ID("column3")},
			rows: [][]sql.Value{
				{sql.Int64Value(1), sql.StringValue("abc"), sql.BoolValue(true)},
				{sql.Int64Value(2), sql.StringValue("def"), sql.BoolValue(false)},
				{sql.Int64Value(3), sql.StringValue("ghi"), sql.BoolValue(true)},
				{sql.Int64Value(4), sql.StringValue("jkl"), sql.BoolValue(false)},
			},
		},
	}

	e, ses := test.StartSession(t)
	for _, c := range cases {
		tx := e.Begin(0)
		if c.values.String() != c.s {
			t.Errorf("(%v).String() got %q want %q", c.values, c.values.String(), c.s)
			continue
		}
		ret, err := c.values.Plan(ses.Context(), ses, e, tx)
		if err != nil {
			t.Errorf("(%v).Plan() failed with %s", c.values, err)
			continue
		}
		rows, ok := ret.(sql.Rows)
		if !ok {
			t.Errorf("(%v).Plan() did not return Rows", c.values)
			continue
		}
		cols := rows.Columns()
		if !reflect.DeepEqual(cols, c.cols) {
			t.Errorf("(%v).Plan().Columns() got %v want %v", c.values, cols, c.cols)
			continue
		}
		all, err := evaluate.AllRows(ses, rows)
		if err != nil {
			t.Errorf("(%v).AllRows() failed with %s", c.values, err)
		}
		if !reflect.DeepEqual(all, c.rows) {
			t.Errorf("(%v).AllRows() got %v want %v", c.values, all, c.rows)
		}

		err = tx.Commit(ses.Context())
		if err != nil {
			t.Error(err)
		}
	}
}
