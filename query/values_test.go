package query_test

import (
	"testing"

	"github.com/leftmike/maho/engine"
	_ "github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/query"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

var started bool

func startEngine(t *testing.T) {
	t.Helper()

	if !started {
		err := engine.Start("basic", "testdata", sql.ID("test"))
		if err != nil {
			t.Fatal(err)
		}
		started = true
	}
}

func TestValues(t *testing.T) {
	cases := []struct {
		values query.Values
		s      string
		cols   []sql.Identifier
		rows   [][]sql.Value
	}{
		{
			query.Values{
				[][]expr.Expr{
					{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True(), expr.Nil()},
				},
			},
			"VALUES (1, 'abc', true, NULL)",
			[]sql.Identifier{sql.ID("column1"), sql.ID("column2"), sql.ID("column3"),
				sql.ID("column4")},
			[][]sql.Value{
				{sql.Int64Value(1), sql.StringValue("abc"), sql.BoolValue(true), nil},
			},
		},
		{
			query.Values{
				[][]expr.Expr{
					{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True()},
					{expr.Int64Literal(2), expr.StringLiteral("def"), expr.False()},
					{expr.Int64Literal(3), expr.StringLiteral("ghi"), expr.True()},
					{expr.Int64Literal(4), expr.StringLiteral("jkl"), expr.False()},
				},
			},
			"VALUES (1, 'abc', true), (2, 'def', false), (3, 'ghi', true), (4, 'jkl', false)",
			[]sql.Identifier{sql.ID("column1"), sql.ID("column2"), sql.ID("column3")},
			[][]sql.Value{
				{sql.Int64Value(1), sql.StringValue("abc"), sql.BoolValue(true)},
				{sql.Int64Value(2), sql.StringValue("def"), sql.BoolValue(false)},
				{sql.Int64Value(3), sql.StringValue("ghi"), sql.BoolValue(true)},
				{sql.Int64Value(4), sql.StringValue("jkl"), sql.BoolValue(false)},
			},
		},
	}

	startEngine(t)
	for _, c := range cases {
		if c.values.String() != c.s {
			t.Errorf("(%v).String() got %q want %q", c.values, c.values.String(), c.s)
			continue
		}
		rows, err := c.values.Rows()
		if err != nil {
			t.Errorf("(%v).Rows() failed with %s", c.values, err)
			continue
		}
		cols := rows.Columns()
		if !testutil.DeepEqual(cols, c.cols) {
			t.Errorf("(%v).Rows().Columns() got %v want %v", c.values, cols, c.cols)
			continue
		}
		all, err := query.AllRows(rows)
		if err != nil {
			t.Errorf("(%v).Rows().Next() failed with %s", c.values, err)
		}
		var trc string
		if !testutil.DeepEqual(all, c.rows, &trc) {
			t.Errorf("(%v).Rows() got %v want %v\n%s", c.values, all, c.rows, trc)
		}
	}
}

func TestFromValues(t *testing.T) {
	cases := []struct {
		from query.FromValues
		s    string
		cols []sql.Identifier
		rows [][]sql.Value
	}{
		{
			query.FromValues{
				query.Values{
					[][]expr.Expr{
						{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True(), expr.Nil()},
					},
				},
				sql.ID("vals"),
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
			},
			"(VALUES (1, 'abc', true, NULL)) AS vals (c1, c2, c3, c4)",
			[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
			[][]sql.Value{
				{sql.Int64Value(1), sql.StringValue("abc"), sql.BoolValue(true), nil},
			},
		},
		{
			query.FromValues{
				query.Values{
					[][]expr.Expr{
						{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True()},
						{expr.Int64Literal(2), expr.StringLiteral("def"), expr.False()},
						{expr.Int64Literal(3), expr.StringLiteral("ghi"), expr.True()},
						{expr.Int64Literal(4), expr.StringLiteral("jkl"), expr.False()},
					},
				},
				sql.ID("vals"),
				[]sql.Identifier{sql.ID("idx"), sql.ID("name"), sql.ID("flag")},
			},
			"(VALUES (1, 'abc', true), (2, 'def', false), (3, 'ghi', true), (4, 'jkl', false)) AS vals (idx, name, flag)",
			[]sql.Identifier{sql.ID("idx"), sql.ID("name"), sql.ID("flag")},
			[][]sql.Value{
				{sql.Int64Value(1), sql.StringValue("abc"), sql.BoolValue(true)},
				{sql.Int64Value(2), sql.StringValue("def"), sql.BoolValue(false)},
				{sql.Int64Value(3), sql.StringValue("ghi"), sql.BoolValue(true)},
				{sql.Int64Value(4), sql.StringValue("jkl"), sql.BoolValue(false)},
			},
		},
	}

	for _, c := range cases {
		if c.from.String() != c.s {
			t.Errorf("(%v).String() got %q want %q", c.from, c.from.String(), c.s)
			continue
		}
		rows, fctx, err := c.from.TestRows()
		if err != nil {
			t.Errorf("(%v).Rows() failed with %s", c.from, err)
			continue
		}
		cols := fctx.TestColumns()
		if !testutil.DeepEqual(cols, c.cols) {
			t.Errorf("(%v).Rows().Columns() got %v want %v", c.from, cols, c.cols)
			continue
		}
		if len(cols) != len(rows.Columns()) {
			t.Errorf("(%v).rows() got %d for len(fctx.columns) and %d for len(rows.Columns())",
				c.from, len(cols), len(rows.Columns()))
			continue
		}
		all, err := query.AllRows(rows)
		if err != nil {
			t.Errorf("(%v).Rows().Next() failed with %s", c.from, err)
		}
		var trc string
		if !testutil.DeepEqual(all, c.rows, &trc) {
			t.Errorf("(%v).Rows() got %v want %v\n%s", c.from, all, c.rows, trc)
		}
	}
}
