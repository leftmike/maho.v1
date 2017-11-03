package test_test

import (
	"maho/db"
	"maho/sql"
	"maho/test"
	"testing"
)

func TestRowsToStmt(t *testing.T) {
	cases := []struct {
		rows db.Rows
		s    string
	}{
		{
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{}),
			"SELECT * FROM (VALUES (NULL, NULL, NULL, NULL)) AS rows (c1, c2, c3, c4) WHERE false",
		},
		{
			test.MakeRows(
				[]sql.Identifier{sql.ID("col1"), sql.ID("col2")},
				[][]sql.Value{
					{nil, true},
					{"abc", false},
				}),
			"SELECT * FROM (VALUES (NULL, true), ('abc', false)) AS rows (col1, col2)",
		},
	}

	for _, c := range cases {
		s, err := test.RowsToStmt(c.rows)
		if err != nil {
			t.Errorf("RowsToStmt(%v) failed with %s", c.rows, err)
		} else if s != c.s {
			t.Errorf("RowsToStmt(%v) got %q want %q", c.rows, s, c.s)
		}
	}
}

func TestRowsEqual(t *testing.T) {
	cases := []struct {
		rows1     db.Rows
		rows2     db.Rows
		identical bool
	}{
		{
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{}),
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{}),
			true,
		},
		{
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{
					{nil, true, int64(1), "abcd"},
					{"xxx", false, int64(1), "efgh"},
				}),
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{
					{nil, true, int64(1), "abcd"},
					{"xxx", false, int64(1), "efgh"},
				}),
			true,
		},
		{
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{
					{nil, true, int64(1), "abcd"},
					{"xxx", false, int64(1), "efgh"},
				}),
			test.MakeRows(
				[]sql.Identifier{sql.ID("d1"), sql.ID("d2"), sql.ID("d3"), sql.ID("d4")},
				[][]sql.Value{
					{nil, true, int64(1), "abcd"},
					{"xxx", false, int64(1), "efgh"},
				}),
			false,
		},
		{
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{
					{nil, true, int64(1), "abcd"},
					{"xxx", false, int64(1), "efgh"},
				}),
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{
					{"xxx", false, int64(1), "efgh"},
					{nil, true, int64(1), "abcd"},
				}),
			true,
		},
	}

	for _, c := range cases {
		if test.RowsEqual(c.rows1, c.rows2) != c.identical {
			t.Errorf("RowsEqual(%v, %v) got %t want %t", c.rows1, c.rows2, !c.identical,
				c.identical)
		}
	}
}

func TestRowsIdentical(t *testing.T) {
	cases := []struct {
		rows1     db.Rows
		rows2     db.Rows
		identical bool
	}{
		{
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{}),
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{}),
			true,
		},
		{
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{
					{nil, true, int64(1), "abcd"},
					{"xxx", false, int64(1), "efgh"},
				}),
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{
					{nil, true, int64(1), "abcd"},
					{"xxx", false, int64(1), "efgh"},
				}),
			true,
		},
		{
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{
					{nil, true, int64(1), "abcd"},
					{"xxx", false, int64(1), "efgh"},
				}),
			test.MakeRows(
				[]sql.Identifier{sql.ID("d1"), sql.ID("d2"), sql.ID("d3"), sql.ID("d4")},
				[][]sql.Value{
					{nil, true, int64(1), "abcd"},
					{"xxx", false, int64(1), "efgh"},
				}),
			false,
		},
		{
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{
					{nil, true, int64(1), "abcd"},
					{"xxx", false, int64(1), "efgh"},
				}),
			test.MakeRows(
				[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				[][]sql.Value{
					{"xxx", false, int64(1), "efgh"},
					{nil, true, int64(1), "abcd"},
				}),
			false,
		},
	}

	for _, c := range cases {
		if test.RowsIdentical(c.rows1, c.rows2) != c.identical {
			t.Errorf("RowsIdentical(%v, %v) got %t want %t", c.rows1, c.rows2, !c.identical,
				c.identical)
		}
	}
}

func TestMakeRows(t *testing.T) {
	cols := []sql.Identifier{sql.ID("col1"), sql.ID("col2"), sql.ID("col3"), sql.ID("col4")}
	vals := [][]sql.Value{
		{int64(1), true, "abc", float64(12.34)},
		{int64(2), true, "def", float64(23.45)},
		{int64(3), true, "ghi", float64(34.56)},
		{int64(4), true, "jkl", float64(45.67)},
		{int64(5), true, "mno", float64(56.78)},
	}

	rows := test.MakeRows(cols, vals)
	if !test.DeepEqual(rows.Columns(), cols) {
		t.Errorf("rows.Columns() got %v want %v", rows.Columns(), cols)
	}
	all, err := test.AllRows(rows)
	if err != nil {
		t.Errorf("AllRows(%v) failed with %s", rows, err)
	} else if !test.DeepEqual(all, vals) {
		t.Errorf("AllRows(%v) got %v want %v", rows, all, vals)
	}
}
