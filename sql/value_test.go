package sql_test

import (
	"testing"

	"github.com/leftmike/maho/sql"
)

func TestCompare(t *testing.T) {
	cases := []struct {
		v1, v2 sql.Value
		cmp    int
	}{
		{nil, sql.BoolValue(true), -1},
		{nil, nil, 0},

		{sql.BoolValue(false), nil, 1},
		{sql.BoolValue(true), sql.BoolValue(true), 0},
		{sql.BoolValue(false), sql.BoolValue(false), 0},
		{sql.BoolValue(false), sql.BoolValue(true), -1},
		{sql.BoolValue(true), sql.BoolValue(false), 1},
		{sql.BoolValue(false), sql.Float64Value(1.23), -1},

		{sql.Float64Value(1.23), sql.BoolValue(false), 1},
		{sql.Float64Value(1.23), sql.Int64Value(123), -1},
		{sql.Float64Value(1.23), sql.StringValue("abc"), -1},
		{sql.Float64Value(1.23), sql.Float64Value(2.34), -1},
		{sql.Float64Value(1.23), sql.Float64Value(1.23), 0},
		{sql.Float64Value(1.23), sql.Float64Value(0.12), 1},

		{sql.Int64Value(123), sql.BoolValue(false), 1},
		{sql.Int64Value(123), sql.Float64Value(1.23), 1},
		{sql.Int64Value(123), sql.StringValue("abc"), -1},
		{sql.Int64Value(123), sql.Int64Value(234), -1},
		{sql.Int64Value(123), sql.Int64Value(123), 0},
		{sql.Int64Value(123), sql.Int64Value(12), 1},

		{sql.StringValue("abc"), sql.BoolValue(false), 1},
		{sql.StringValue("abc"), sql.Float64Value(1.23), 1},
		{sql.StringValue("abc"), sql.Int64Value(123), 1},
		{sql.StringValue("def"), sql.StringValue("ghi"), -1},
		{sql.StringValue("def"), sql.StringValue("def"), 0},
		{sql.StringValue("def"), sql.StringValue("abc"), 1},
	}

	for _, c := range cases {
		cmp := sql.Compare(c.v1, c.v2)
		if cmp != c.cmp {
			t.Errorf("Compare(%v, %v) got %d want %d", c.v1, c.v2, cmp, c.cmp)
		}
	}
}
