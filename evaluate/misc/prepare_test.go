package misc_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/evaluate/misc"
	"github.com/leftmike/maho/evaluate/query"
	"github.com/leftmike/maho/evaluate/test"
	"github.com/leftmike/maho/sql"
)

func TestPreparePlan(t *testing.T) {
	type prepareTest struct {
		params []sql.Value
		rows   [][]sql.Value
		fail   bool
	}
	cases := []struct {
		stmt  misc.Prepare
		s     string
		fail  bool
		tests []prepareTest
	}{
		{
			stmt: misc.Prepare{
				Name: sql.ID("test"),
				Stmt: &query.Select{
					From: query.FromStmt{
						Stmt: &query.Values{
							Expressions: [][]expr.Expr{
								{expr.Param{1}, expr.Param{2}},
							},
						},
						Alias: sql.ID("vals"),
						ColumnAliases: []sql.Identifier{
							sql.ID("c1"), sql.ID("c2")},
					},
				},
			},
			s: "PREPARE test AS SELECT * FROM (VALUES ($1, $2)) AS vals (c1, c2)",
			tests: []prepareTest{
				{
					params: []sql.Value{sql.Int64Value(10), sql.Int64Value(20)},
					rows: [][]sql.Value{
						{sql.Int64Value(10), sql.Int64Value(20)},
					},
				},
				{
					params: []sql.Value{sql.Int64Value(20), sql.Int64Value(10)},
					rows: [][]sql.Value{
						{sql.Int64Value(20), sql.Int64Value(10)},
					},
				},
				{
					params: []sql.Value{sql.StringValue("abc"), sql.StringValue("def")},
					rows: [][]sql.Value{
						{sql.StringValue("abc"), sql.StringValue("def")},
					},
				},
				{
					params: []sql.Value{sql.Int64Value(10)},
					fail:   true,
				},
				{
					params: []sql.Value{sql.Int64Value(10), sql.Int64Value(20), nil},
					fail:   true,
				},
			},
		},
		{
			stmt: misc.Prepare{
				Name: sql.ID("test"),
				Stmt: &query.Select{
					From: query.FromStmt{
						Stmt: &query.Values{
							Expressions: [][]expr.Expr{
								{expr.Param{5}, expr.Param{2}},
							},
						},
						Alias: sql.ID("vals"),
						ColumnAliases: []sql.Identifier{
							sql.ID("c1"), sql.ID("c2")},
					},
				},
			},
			s:    "PREPARE test AS SELECT * FROM (VALUES ($5, $2)) AS vals (c1, c2)",
			fail: true,
		},
		{
			stmt: misc.Prepare{
				Name: sql.ID("test"),
				Stmt: &query.Select{
					From: query.FromStmt{
						Stmt: &query.Values{
							Expressions: [][]expr.Expr{
								{expr.Param{1}, expr.Param{1}},
								{expr.Param{2},
									&expr.Binary{
										Op:    expr.MultiplyOp,
										Left:  expr.Param{2},
										Right: expr.Int64Literal(2),
									},
								},
								{expr.Param{3},
									&expr.Binary{
										Op:    expr.MultiplyOp,
										Left:  expr.Param{3},
										Right: expr.Int64Literal(3),
									},
								},
								{expr.Param{4},
									&expr.Binary{
										Op:    expr.MultiplyOp,
										Left:  expr.Param{4},
										Right: expr.Int64Literal(4),
									},
								},
							},
						},
						Alias: sql.ID("vals"),
						ColumnAliases: []sql.Identifier{
							sql.ID("c1"), sql.ID("c2")},
					},
				},
			},
			s: "PREPARE test AS SELECT * FROM " +
				"(VALUES ($1, $1), ($2, ($2 * 2)), ($3, ($3 * 3)), ($4, ($4 * 4))) " +
				"AS vals (c1, c2)",
			tests: []prepareTest{
				{
					params: []sql.Value{sql.Int64Value(5), sql.Int64Value(6), sql.Int64Value(7),
						sql.Int64Value(8)},
					rows: [][]sql.Value{
						{sql.Int64Value(5), sql.Int64Value(5)},
						{sql.Int64Value(6), sql.Int64Value(12)},
						{sql.Int64Value(7), sql.Int64Value(21)},
						{sql.Int64Value(8), sql.Int64Value(32)},
					},
				},
			},
		},
		{
			stmt: misc.Prepare{
				Name: sql.ID("test"),
				Stmt: &query.Select{
					From: query.FromStmt{
						Stmt: &query.Values{
							Expressions: [][]expr.Expr{
								{expr.Param{1}, expr.Param{1}},
								{expr.Param{2},
									&expr.Binary{
										Op:    expr.MultiplyOp,
										Left:  expr.Param{2},
										Right: expr.Int64Literal(2),
									},
								},
								{expr.Param{3},
									&expr.Binary{
										Op:    expr.MultiplyOp,
										Left:  expr.Param{3},
										Right: expr.Int64Literal(3),
									},
								},
								{expr.Param{4},
									&expr.Binary{
										Op:    expr.MultiplyOp,
										Left:  expr.Param{4},
										Right: expr.Int64Literal(4),
									},
								},
							},
						},
						Alias: sql.ID("vals"),
						ColumnAliases: []sql.Identifier{
							sql.ID("c1"), sql.ID("c2")},
					},
					Where: &expr.Binary{
						Op:    expr.EqualOp,
						Left:  expr.Ref{sql.ID("c1")},
						Right: expr.Param{5},
					},
				},
			},
			s: "PREPARE test AS SELECT * FROM " +
				"(VALUES ($1, $1), ($2, ($2 * 2)), ($3, ($3 * 3)), ($4, ($4 * 4))) " +
				"AS vals (c1, c2) WHERE (c1 == $5)",
			tests: []prepareTest{
				{
					params: []sql.Value{sql.Int64Value(5), sql.Int64Value(6), sql.Int64Value(7),
						sql.Int64Value(8), sql.Int64Value(6)},
					rows: [][]sql.Value{
						{sql.Int64Value(6), sql.Int64Value(12)},
					},
				},
				{
					params: []sql.Value{sql.Int64Value(5), sql.Int64Value(6), sql.Int64Value(7),
						sql.Int64Value(8), sql.Int64Value(8)},
					rows: [][]sql.Value{
						{sql.Int64Value(8), sql.Int64Value(32)},
					},
				},
			},
		},
		{
			stmt: misc.Prepare{
				Name: sql.ID("test"),
				Stmt: &query.Select{
					From: query.FromStmt{
						Stmt: &query.Values{
							Expressions: [][]expr.Expr{
								{expr.Param{1}, expr.Param{1}},
								{expr.Param{2},
									&expr.Binary{
										Op:    expr.MultiplyOp,
										Left:  expr.Param{2},
										Right: expr.Int64Literal(2),
									},
								},
								{expr.Param{3},
									&expr.Binary{
										Op:    expr.MultiplyOp,
										Left:  expr.Param{3},
										Right: expr.Int64Literal(3),
									},
								},
								{expr.Param{4},
									&expr.Binary{
										Op:    expr.MultiplyOp,
										Left:  expr.Param{4},
										Right: expr.Int64Literal(4),
									},
								},
							},
						},
						Alias: sql.ID("vals"),
						ColumnAliases: []sql.Identifier{
							sql.ID("c1"), sql.ID("c2")},
					},
					Where: &expr.Binary{
						Op: expr.AndOp,
						Left: &expr.Binary{
							Op:    expr.GreaterThanOp,
							Left:  expr.Ref{sql.ID("c2")},
							Right: expr.Param{5},
						},
						Right: &expr.Binary{
							Op:    expr.LessThanOp,
							Left:  expr.Ref{sql.ID("c2")},
							Right: expr.Param{6},
						},
					},
				},
			},
			s: "PREPARE test AS SELECT * FROM " +
				"(VALUES ($1, $1), ($2, ($2 * 2)), ($3, ($3 * 3)), ($4, ($4 * 4))) " +
				"AS vals (c1, c2) WHERE ((c2 > $5) AND (c2 < $6))",

			tests: []prepareTest{
				{
					params: []sql.Value{sql.Int64Value(5), sql.Int64Value(6), sql.Int64Value(7),
						sql.Int64Value(8), sql.Int64Value(4), sql.Int64Value(21)},
					rows: [][]sql.Value{
						{sql.Int64Value(5), sql.Int64Value(5)},
						{sql.Int64Value(6), sql.Int64Value(12)},
					},
				},
				{
					params: []sql.Value{sql.Int64Value(5), sql.Int64Value(6), sql.Int64Value(7),
						sql.Int64Value(8), sql.Int64Value(5), sql.Int64Value(36)},
					rows: [][]sql.Value{
						{sql.Int64Value(6), sql.Int64Value(12)},
						{sql.Int64Value(7), sql.Int64Value(21)},
						{sql.Int64Value(8), sql.Int64Value(32)},
					},
				},
			},
		},
	}

	ctx := context.Background()
	e, ses := test.StartSession(t)
	for _, c := range cases {
		tx := e.Begin(0)
		if c.stmt.String() != c.s {
			t.Errorf("(%v).String() got %q want %q", c.stmt, c.stmt.String(), c.s)
		}
		prep, err := evaluate.PreparePlan(ctx, c.stmt.Stmt, ses, tx)
		if c.fail {
			if err == nil {
				t.Errorf("(%v).PreparePlan() did not fail", c.stmt)
			}
			err = tx.Commit(ctx)
			if err != nil {
				t.Errorf("Commit failed with %s", err)
			}
			continue
		} else if err != nil {
			t.Errorf("(%v).PreparePlan() failed with %s", c.stmt, err)
		}
		prepRows, ok := prep.(*evaluate.PreparedRowsPlan)
		if !ok {
			t.Errorf("(%v).PreparePlan() did not return PreparedRows", c.stmt)
		}

		for tdx, tst := range c.tests {
			err = prepRows.SetParameters(tst.params)
			if tst.fail {
				if err == nil {
					t.Errorf("%v[%d]: SetParameters did not fail", c.stmt, tdx)
				}
				continue
			} else if err != nil {
				t.Errorf("%v[%d]: SetParameters failed with %s", c.stmt, tdx, err)
			}
			rows, err := prepRows.Rows(ctx, tx, nil)
			if err != nil {
				t.Errorf("%v[%d]: Rows failed with %s", c.stmt, tdx, err)
			}
			all, err := evaluate.AllRows(ctx, rows)
			if err != nil {
				t.Errorf("%v[%d]: AllRows() failed with %s", c.stmt, tdx, err)
			}
			if !reflect.DeepEqual(all, tst.rows) {
				t.Errorf("%v[%d]: AllRows() got %v want %v", c.stmt, tdx, all, tst.rows)
			}
		}

		err = tx.Commit(ctx)
		if err != nil {
			t.Errorf("Commit failed with %s", err)
		}
	}
}
