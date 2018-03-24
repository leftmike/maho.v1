package mvcc

import (
	"math"
	"strings"
	"testing"

	"github.com/leftmike/maho/sql"
)

func TestEncodedLength(t *testing.T) {
	cases := []struct {
		col  int
		v    sql.Value
		l    int
		fail bool
	}{
		{0, nil, 2, false},
		{100, nil, 2, false},
		{222, nil, 2, false},
		{-1, nil, 0, true},
		{256, nil, 0, true},
		{0, sql.BoolValue(true), 2, false},
		{1, sql.BoolValue(false), 2, false},
		{22, sql.Int64Value(-1), 10, false},
		{33, sql.Float64Value(1234.5678E9), 10, false},
		{4, sql.StringValue(""), 4, false},
		{123, sql.StringValue("abcd"), 8, false},
		{4, sql.StringValue(strings.Repeat("x", 1020)), 1024, false},
		{4, sql.StringValue(strings.Repeat("x", math.MaxUint16)), math.MaxUint16 + 4, false},
		{4, sql.StringValue(strings.Repeat("x", math.MaxUint16+1)), 0, true},
	}

	for _, c := range cases {
		l, err := encodedLength(c.col, c.v)
		if c.fail {
			if err == nil {
				t.Errorf("encodedLength(%d, %s) did not fail", c.col, c.v)
			}
		} else if err != nil {
			t.Errorf("encodedLength(%d, %s) failed with %s", c.col, c.v, err)
		} else if l != c.l {
			t.Errorf("encodedLength(%d, %s) got %d want %d", c.col, c.v, l, c.l)
		}
	}
}

func TestRecord(t *testing.T) {
	cases := []struct {
		col int
		v   sql.Value
	}{
		{3, sql.BoolValue(true)},
		{10, nil},
		{17, sql.Int64Value(123456)},
		{18, sql.BoolValue(false)},
		{23, sql.Float64Value(9876.5432E-123)},
		{45, sql.StringValue("abcdefghijklmnopqrstuvwxyz")},
		{46, nil},
		{47, sql.Int64Value(-123456)},
	}

	bl := 0
	for _, c := range cases {
		el, err := encodedLength(c.col, c.v)
		if err != nil {
			t.Errorf("encodedLength(%d, %s) failed with %s", c.col, c.v, err)
		}
		bl += el
	}

	buf := make([]byte, bl)
	b := buf
	for _, c := range cases {
		b = encodeColVal(c.col, c.v, b)
	}

	b = buf
	for _, c := range cases {
		var (
			col int
			v   sql.Value
			err error
		)
		b, col, v, err = decodeColVal(b)
		if err != nil {
			t.Errorf("decodeColVal() failed with %s", err)
		} else {
			if col != c.col {
				t.Errorf("column got %d want %d", col, c.col)
			}
			if v != c.v {
				t.Errorf("value got %s want %s", v, c.v)
			}
		}
	}
}
