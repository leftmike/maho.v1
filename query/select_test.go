package query_test

import (
	"testing"

	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/query"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

func TestSelect(t *testing.T) {
	cases := []struct {
		stmt query.Select
		s    string
		cols []sql.Identifier
		rows [][]sql.Value
	}{
		{
			query.Select{
				From: query.FromValues{
					query.Values{
						[][]expr.Expr{
							{&expr.Literal{int64(1)}, &expr.Literal{"abc"}, &expr.Literal{true},
								&expr.Literal{nil}},
						},
					},
					sql.ID("vals"),
					[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				},
			},
			"SELECT * FROM (VALUES (1, 'abc', true, NULL)) AS vals (c1, c2, c3, c4)",
			[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
			[][]sql.Value{
				{int64(1), "abc", true, nil},
			},
		},
		{
			query.Select{
				From: query.FromValues{
					query.Values{
						[][]expr.Expr{
							{&expr.Literal{int64(1)}, &expr.Literal{"abc"},
								&expr.Literal{true}},
							{&expr.Literal{int64(2)}, &expr.Literal{"def"},
								&expr.Literal{false}},
							{&expr.Literal{int64(3)}, &expr.Literal{"ghi"},
								&expr.Literal{true}},
							{&expr.Literal{int64(4)}, &expr.Literal{"jkl"},
								&expr.Literal{false}},
						},
					},
					sql.ID("vals"),
					[]sql.Identifier{sql.ID("idx"), sql.ID("name"), sql.ID("flag")},
				},
			},
			"SELECT * FROM (VALUES (1, 'abc', true), (2, 'def', false), (3, 'ghi', true), (4, 'jkl', false)) AS vals (idx, name, flag)",
			[]sql.Identifier{sql.ID("idx"), sql.ID("name"), sql.ID("flag")},
			[][]sql.Value{
				{int64(1), "abc", true},
				{int64(2), "def", false},
				{int64(3), "ghi", true},
				{int64(4), "jkl", false},
			},
		},
		{
			query.Select{
				From: query.FromValues{
					query.Values{
						[][]expr.Expr{
							{&expr.Literal{nil}, &expr.Literal{nil}, &expr.Literal{nil}},
						},
					},
					sql.ID("vals"),
					[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")},
				},
				Where: &expr.Literal{false},
			},
			"SELECT * FROM (VALUES (NULL, NULL, NULL)) AS vals (c1, c2, c3) WHERE false",
			[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3")},
			[][]sql.Value{},
		},
	}

	e, _, err := testutil.StartEngine("test")
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range cases {
		if c.stmt.String() != c.s {
			t.Errorf("(%v).String() got %q want %q", c.stmt, c.stmt.String(), c.s)
			continue
		}
		rows, err := c.stmt.Rows(e)
		if err != nil {
			t.Errorf("(%v).Rows() failed with %s", c.stmt, err)
			continue
		}
		cols := rows.Columns()
		if !testutil.DeepEqual(cols, c.cols) {
			t.Errorf("(%v).Rows().Columns() got %v want %v", c.stmt, cols, c.cols)
			continue
		}
		all, err := query.AllRows(rows)
		if err != nil {
			t.Errorf("(%v).Rows().Next() failed with %s", c.stmt, err)
		}
		var trc string
		if !testutil.DeepEqual(all, c.rows, &trc) {
			t.Errorf("(%v).Rows() got %v want %v\n%s", c.stmt, all, c.rows, trc)
		}
	}
}
