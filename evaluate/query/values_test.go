package query_test

import (
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/evaluate/query"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage/basic"
	"github.com/leftmike/maho/testutil"
)

func startEngine(t *testing.T) sql.Engine {
	t.Helper()

	st, err := basic.NewStore("testdata")
	if err != nil {
		t.Fatal(err)
	}
	e := engine.NewEngine(st)

	err = e.CreateDatabase(sql.ID("test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	return e
}

func TestValues(t *testing.T) {
	cases := []struct {
		values query.Values
		s      string
		cols   []sql.Identifier
		rows   [][]sql.Value
	}{
		{
			values: query.Values{
				Expressions: [][]sql.Expr{
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
				Expressions: [][]sql.Expr{
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

	e := startEngine(t)
	ses := &evaluate.Session{
		Engine:          e,
		DefaultDatabase: sql.ID("test"),
		DefaultSchema:   sql.PUBLIC,
	}
	for _, c := range cases {
		tx := e.Begin(0)
		if c.values.String() != c.s {
			t.Errorf("(%v).String() got %q want %q", c.values, c.values.String(), c.s)
			continue
		}
		ret, err := c.values.Plan(ses, tx)
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
		if !testutil.DeepEqual(cols, c.cols) {
			t.Errorf("(%v).Plan().Columns() got %v want %v", c.values, cols, c.cols)
			continue
		}
		all, err := evaluate.AllRows(ses, rows)
		if err != nil {
			t.Errorf("(%v).AllRows() failed with %s", c.values, err)
		}
		var trc string
		if !testutil.DeepEqual(all, c.rows, &trc) {
			t.Errorf("(%v).AllRows() got %v want %v\n%s", c.values, all, c.rows, trc)
		}

		err = tx.Commit(ses.Context())
		if err != nil {
			t.Error(err)
		}
	}
}
