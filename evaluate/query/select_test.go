package query_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/evaluate/query"
	"github.com/leftmike/maho/evaluate/test"
	"github.com/leftmike/maho/sql"
)

func TestSelectString(t *testing.T) {
	s := query.Select{
		From: &query.FromTableAlias{
			TableName: sql.TableName{sql.ID("db"), sql.ID("sc"), sql.ID("tbl")},
			Alias:     sql.ID("alias"),
		},
	}
	r := "SELECT * FROM db.sc.tbl AS alias"
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
				From: query.FromStmt{
					Stmt: &query.Values{
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
				From: query.FromStmt{
					Stmt: &query.Values{
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
				From: query.FromStmt{
					Stmt: &query.Values{
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

	ctx := context.Background()
	e, ses := test.StartSession(t)
	for _, c := range cases {
		tx := e.Begin(0)
		if c.stmt.String() != c.s {
			t.Errorf("(%v).String() got %q want %q", c.stmt, c.stmt.String(), c.s)
			continue
		}
		plan, err := c.stmt.Plan(ctx, ses, tx)
		if err != nil {
			t.Errorf("(%v).Plan() failed with %s", c.stmt, err)
			continue
		}
		rowsPlan, ok := plan.(evaluate.RowsPlan)
		if !ok {
			t.Errorf("(%v).Plan() did not return Rows", c.stmt)
			continue
		}
		rows, err := rowsPlan.Rows(ctx, tx)
		if err != nil {
			t.Errorf("(%v).Rows() failed with %s", c.stmt, err)
			continue
		}

		cols := rowsPlan.Columns()
		if !reflect.DeepEqual(cols, c.cols) {
			t.Errorf("(%v).Plan().Columns() got %v want %v", c.stmt, cols, c.cols)
			continue
		}
		all, err := evaluate.AllRows(ctx, rows)
		if err != nil {
			t.Errorf("(%v).AllRows() failed with %s", c.stmt, err)
		}
		if !reflect.DeepEqual(all, c.rows) {
			t.Errorf("(%v).AllRows() got %v want %v", c.stmt, all, c.rows)
		}

		err = tx.Commit(ctx)
		if err != nil {
			t.Error(err)
		}
	}
}
