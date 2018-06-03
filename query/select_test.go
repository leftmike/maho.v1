package query_test

import (
	"testing"

	"github.com/leftmike/maho/engine"
	_ "github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/execute"
	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/query"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

func TestSelectString(t *testing.T) {
	s := query.Select{
		From: query.FromTableAlias{
			Database: sql.ID("db"),
			Table:    sql.ID("tbl"),
			Alias:    sql.ID("alias"),
		},
	}
	r := "SELECT * FROM db.tbl AS alias"
	if s.String() != r {
		t.Errorf("Select{}.String() got %s want %s", s.String(), r)
	}
}

func TestSelect(t *testing.T) {
	cases := []struct {
		stmt query.Select
		s    string
		cols []sql.Identifier
		rows [][]sql.Value
	}{
		{
			stmt: query.Select{
				From: query.FromValues{
					Values: query.Values{
						Expressions: [][]expr.Expr{
							{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True(),
								expr.Nil()},
						},
					},
					Alias: sql.ID("vals"),
					ColumnAliases: []sql.Identifier{
						sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				},
			},
			s:    "SELECT * FROM (VALUES (1, 'abc', true, NULL)) AS vals (c1, c2, c3, c4)",
			cols: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
			rows: [][]sql.Value{
				{sql.Int64Value(1), sql.StringValue("abc"), sql.BoolValue(true), nil},
			},
		},
		{
			stmt: query.Select{
				From: query.FromValues{
					Values: query.Values{
						Expressions: [][]expr.Expr{
							{expr.Int64Literal(1), expr.StringLiteral("abc"), expr.True()},
							{expr.Int64Literal(2), expr.StringLiteral("def"), expr.False()},
							{expr.Int64Literal(3), expr.StringLiteral("ghi"), expr.True()},
							{expr.Int64Literal(4), expr.StringLiteral("jkl"), expr.False()},
						},
					},
					Alias:         sql.ID("vals"),
					ColumnAliases: []sql.Identifier{sql.ID("idx"), sql.ID("name"), sql.ID("flag")},
				},
			},
			s:    "SELECT * FROM (VALUES (1, 'abc', true), (2, 'def', false), (3, 'ghi', true), (4, 'jkl', false)) AS vals (idx, name, flag)",
			cols: []sql.Identifier{sql.ID("idx"), sql.ID("name"), sql.ID("flag")},
			rows: [][]sql.Value{
				{sql.Int64Value(1), sql.StringValue("abc"), sql.BoolValue(true)},
				{sql.Int64Value(2), sql.StringValue("def"), sql.BoolValue(false)},
				{sql.Int64Value(3), sql.StringValue("ghi"), sql.BoolValue(true)},
				{sql.Int64Value(4), sql.StringValue("jkl"), sql.BoolValue(false)},
			},
		},
		{
			stmt: query.Select{
				From: query.FromValues{
					Values: query.Values{
						Expressions: [][]expr.Expr{
							{expr.Nil(), expr.Nil(), expr.Nil()},
						},
					},
					Alias:         sql.ID("vals"),
					ColumnAliases: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")},
				},
				Where: expr.False(),
			},
			s:    "SELECT * FROM (VALUES (NULL, NULL, NULL)) AS vals (c1, c2, c3) WHERE false",
			cols: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")},
			rows: [][]sql.Value{},
		},
	}

	startEngine(t)
	tx := engine.Begin()
	ses := execute.NewSession("basic", sql.ID("test"))
	for _, c := range cases {
		if c.stmt.String() != c.s {
			t.Errorf("(%v).String() got %q want %q", c.stmt, c.stmt.String(), c.s)
			continue
		}
		rows, err := c.stmt.Rows(ses, tx)
		if err != nil {
			t.Errorf("(%v).Rows() failed with %s", c.stmt, err)
			continue
		}
		cols := rows.Columns()
		if !testutil.DeepEqual(cols, c.cols) {
			t.Errorf("(%v).Rows().Columns() got %v want %v", c.stmt, cols, c.cols)
			continue
		}
		all, err := query.AllRows(ses, rows)
		if err != nil {
			t.Errorf("(%v).Rows().Next() failed with %s", c.stmt, err)
		}
		var trc string
		if !testutil.DeepEqual(all, c.rows, &trc) {
			t.Errorf("(%v).Rows() got %v want %v\n%s", c.stmt, all, c.rows, trc)
		}
	}
}
