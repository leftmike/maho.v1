package query_test

import (
	"io"
	"testing"

	"maho/expr"
	"maho/query"
	"maho/sql"
	"maho/test"
)

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
					{&expr.Literal{int64(1)}, &expr.Literal{"abc"}, &expr.Literal{true},
						&expr.Literal{nil}},
				},
			},
			"VALUES (1, 'abc', true, NULL)",
			[]sql.Identifier{sql.ID("column1"), sql.ID("column2"), sql.ID("column3"),
				sql.ID("column4")},
			[][]sql.Value{
				{int64(1), "abc", true, nil},
			},
		},
		{
			query.Values{
				[][]expr.Expr{
					{&expr.Literal{int64(1)}, &expr.Literal{"abc"}, &expr.Literal{true}},
					{&expr.Literal{int64(2)}, &expr.Literal{"def"}, &expr.Literal{false}},
					{&expr.Literal{int64(3)}, &expr.Literal{"ghi"}, &expr.Literal{true}},
					{&expr.Literal{int64(4)}, &expr.Literal{"jkl"}, &expr.Literal{false}},
				},
			},
			"VALUES (1, 'abc', true), (2, 'def', false), (3, 'ghi', true), (4, 'jkl', false)",
			[]sql.Identifier{sql.ID("column1"), sql.ID("column2"), sql.ID("column3")},
			[][]sql.Value{
				{int64(1), "abc", true},
				{int64(2), "def", false},
				{int64(3), "ghi", true},
				{int64(4), "jkl", false},
			},
		},
	}

	e, _, err := test.StartEngine("test")
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range cases {
		if c.values.String() != c.s {
			t.Errorf("(%v).String() got %q want %q", c.values, c.values.String(), c.s)
			continue
		}
		rows, err := c.values.Rows(e)
		if err != nil {
			t.Errorf("(%v).Rows() failed with %s", c.values, err)
			continue
		}
		cols := rows.Columns()
		if !test.DeepEqual(cols, c.cols) {
			t.Errorf("(%v).Rows().Columns() got %v want %v", c.values, cols, c.cols)
			continue
		}
		var all [][]sql.Value
		for {
			dest := make([]sql.Value, len(cols))
			err := rows.Next(dest)
			if err != nil {
				if err != io.EOF {
					t.Errorf("(%v).Rows().Next() failed with %s", c.values, err)
				}
				break
			}
			all = append(all, dest)
		}
		if !test.DeepEqual(all, c.rows) {
			t.Errorf("(%v).Rows() got %v want %v", c.values, all, c.rows)
		}
	}
}

func TestFromValues(t *testing.T) {

}
