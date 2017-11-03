package query

import (
	"testing"

	"maho/expr"
	"maho/sql"
	"maho/test"
)

func TestValues(t *testing.T) {
	cases := []struct {
		values Values
		s      string
		cols   []sql.Identifier
		rows   [][]sql.Value
	}{
		{
			Values{
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
			Values{
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
		all, err := test.AllRows(rows)
		if err != nil {
			t.Errorf("(%v).Rows().Next() failed with %s", c.values, err)
		}
		var trc string
		if !test.DeepEqual(all, c.rows, &trc) {
			t.Errorf("(%v).Rows() got %v want %v\n%s", c.values, all, c.rows, trc)
		}
	}
}

func TestFromValues(t *testing.T) {
	cases := []struct {
		from FromValues
		s    string
		cols []sql.Identifier
		rows [][]sql.Value
	}{
		{
			FromValues{
				Values{
					[][]expr.Expr{
						{&expr.Literal{int64(1)}, &expr.Literal{"abc"}, &expr.Literal{true},
							&expr.Literal{nil}},
					},
				},
				sql.ID("vals"),
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
			},
			"(VALUES (1, 'abc', true, NULL)) AS vals (c1, c2, c3, c4)",
			[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
			[][]sql.Value{
				{int64(1), "abc", true, nil},
			},
		},
		{
			FromValues{
				Values{
					[][]expr.Expr{
						{&expr.Literal{int64(1)}, &expr.Literal{"abc"}, &expr.Literal{true}},
						{&expr.Literal{int64(2)}, &expr.Literal{"def"}, &expr.Literal{false}},
						{&expr.Literal{int64(3)}, &expr.Literal{"ghi"}, &expr.Literal{true}},
						{&expr.Literal{int64(4)}, &expr.Literal{"jkl"}, &expr.Literal{false}},
					},
				},
				sql.ID("vals"),
				[]sql.Identifier{sql.ID("idx"), sql.ID("name"), sql.ID("flag")},
			},
			"(VALUES (1, 'abc', true), (2, 'def', false), (3, 'ghi', true), (4, 'jkl', false)) AS vals (idx, name, flag)",
			[]sql.Identifier{sql.ID("idx"), sql.ID("name"), sql.ID("flag")},
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
		if c.from.String() != c.s {
			t.Errorf("(%v).String() got %q want %q", c.from, c.from.String(), c.s)
			continue
		}
		rows, fctx, err := c.from.rows(e)
		if err != nil {
			t.Errorf("(%v).Rows() failed with %s", c.from, err)
			continue
		}
		cols := fctx.columns()
		if !test.DeepEqual(cols, c.cols) {
			t.Errorf("(%v).Rows().Columns() got %v want %v", c.from, cols, c.cols)
			continue
		}
		if len(cols) != len(rows.Columns()) {
			t.Errorf("(%v).rows() got %d for len(fctx.columns) and %d for len(rows.Columns())",
				c.from, len(cols), len(rows.Columns()))
			continue
		}
		all, err := test.AllRows(rows)
		if err != nil {
			t.Errorf("(%v).Rows().Next() failed with %s", c.from, err)
		}
		var trc string
		if !test.DeepEqual(all, c.rows, &trc) {
			t.Errorf("(%v).Rows() got %v want %v\n%s", c.from, all, c.rows, trc)
		}
	}
}
