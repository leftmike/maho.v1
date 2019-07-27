package bbolt_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/bbolt"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

type testCase struct {
	row     []sql.Value
	colKeys []engine.ColumnKey
	ret     []byte
}

func testMakeKey(t *testing.T, cases []testCase) {
	t.Helper()

	var prevKey []byte
	for i, c := range cases {
		key := bbolt.MakeKey(c.row, c.colKeys)
		if bytes.Compare(key, c.ret) != 0 {
			t.Errorf("MakeKey(%d) got %v want %v", i, key, c.ret)
		}
		if bytes.Compare(prevKey, key) >= 0 {
			t.Errorf("MakeKey(%d) keys not ordered correctly", i)
		}
	}
}

func TestMakeKey(t *testing.T) {
	testMakeKey(t,
		[]testCase{
			{
				row:     []sql.Value{nil},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{0},
			},
			{
				row: []sql.Value{nil},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{0, 0},
			},
			{
				row:     []sql.Value{sql.BoolValue(false)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 0},
			},
			{
				row:     []sql.Value{sql.BoolValue(true)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 1},
			},
			{
				row:     []sql.Value{sql.Int64Value(-456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{2, 255, 255, 255, 255, 255, 255, 254, 56},
			},
			{
				row:     []sql.Value{sql.Int64Value(-123)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{2, 255, 255, 255, 255, 255, 255, 255, 133},
			},
			{
				row:     []sql.Value{sql.Int64Value(0)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{3, 0, 0, 0, 0, 0, 0, 0, 0},
			},
			{
				row:     []sql.Value{sql.Int64Value(123)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{3, 0, 0, 0, 0, 0, 0, 0, 123},
			},
			{
				row:     []sql.Value{sql.Int64Value(456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{3, 0, 0, 0, 0, 0, 0, 1, 200},
			},
			{
				row:     []sql.Value{sql.Float64Value(math.NaN())},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{4},
			},
			{
				row:     []sql.Value{sql.Float64Value(-456.789)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{5, 63, 131, 115, 96, 65, 137, 55, 75},
			},
			{
				row:     []sql.Value{sql.Float64Value(-123.456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{5, 63, 161, 34, 208, 229, 96, 65, 136},
			},
			{
				row:     []sql.Value{sql.Float64Value(0.0)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{6},
			},
			{
				row:     []sql.Value{sql.Float64Value(123.456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{7, 64, 94, 221, 47, 26, 159, 190, 119},
			},
			{
				row:     []sql.Value{sql.Float64Value(456.789)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{7, 64, 124, 140, 159, 190, 118, 200, 180},
			},
			{
				row:     []sql.Value{sql.StringValue([]byte{0, 1, 2, 3, 4})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{8, 1, 0, 1, 1, 2, 3, 4, 0},
			},
			{
				row:     []sql.Value{sql.StringValue("ABCD")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{8, 65, 66, 67, 68, 0},
			},
			{
				row:     []sql.Value{sql.StringValue("ab")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{8, 97, 98, 0},
			},
			{
				row:     []sql.Value{sql.StringValue("abc")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{8, 97, 98, 99, 0},
			},
			{
				row:     []sql.Value{sql.StringValue("abcd")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{8, 97, 98, 99, 100, 0},
			},
		})

	row := []sql.Value{nil, sql.BoolValue(false), sql.BoolValue(true), sql.Int64Value(-456),
		sql.Int64Value(-123), sql.Int64Value(0), sql.Int64Value(123), sql.Int64Value(456),
		sql.Float64Value(math.NaN()), sql.Float64Value(-456.789), sql.Float64Value(-123.456),
		sql.Float64Value(0.0), sql.Float64Value(123.456), sql.Float64Value(456.789),
		sql.StringValue([]byte{0, 1, 2, 3, 4}), sql.StringValue("ABCD"), sql.StringValue("ab"),
		sql.StringValue("abc"), sql.StringValue("abcd")}

	testMakeKey(t,
		[]testCase{
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(1, false)},
				ret:     []byte{1, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(2, false)},
				ret:     []byte{1, 1},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(3, false)},
				ret:     []byte{2, 255, 255, 255, 255, 255, 255, 254, 56},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(4, false)},
				ret:     []byte{2, 255, 255, 255, 255, 255, 255, 255, 133},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(5, false)},
				ret:     []byte{3, 0, 0, 0, 0, 0, 0, 0, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(6, false)},
				ret:     []byte{3, 0, 0, 0, 0, 0, 0, 0, 123},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(7, false)},
				ret:     []byte{3, 0, 0, 0, 0, 0, 0, 1, 200},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(8, false)},
				ret:     []byte{4},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(9, false)},
				ret:     []byte{5, 63, 131, 115, 96, 65, 137, 55, 75},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(10, false)},
				ret:     []byte{5, 63, 161, 34, 208, 229, 96, 65, 136},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(11, false)},
				ret:     []byte{6},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(12, false)},
				ret:     []byte{7, 64, 94, 221, 47, 26, 159, 190, 119},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(13, false)},
				ret:     []byte{7, 64, 124, 140, 159, 190, 118, 200, 180},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(14, false)},
				ret:     []byte{8, 1, 0, 1, 1, 2, 3, 4, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(15, false)},
				ret:     []byte{8, 65, 66, 67, 68, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(16, false)},
				ret:     []byte{8, 97, 98, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(17, false)},
				ret:     []byte{8, 97, 98, 99, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(18, false)},
				ret:     []byte{8, 97, 98, 99, 100, 0},
			},
		})

	testMakeKey(t,
		[]testCase{
			{
				row: []sql.Value{sql.BoolValue(true), nil},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BoolValue(false)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 1, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BoolValue(true)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 1, 1},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(-456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 2, 255, 255, 255, 255, 255, 255, 254, 56},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(-123)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 2, 255, 255, 255, 255, 255, 255, 255, 133},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(0)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 3, 0, 0, 0, 0, 0, 0, 0, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(123)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 3, 0, 0, 0, 0, 0, 0, 0, 123},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 3, 0, 0, 0, 0, 0, 0, 1, 200},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(math.NaN())},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 4},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(-456.789)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 5, 63, 131, 115, 96, 65, 137, 55, 75},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(-123.456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 5, 63, 161, 34, 208, 229, 96, 65, 136},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(0.0)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 6},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(123.456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 7, 64, 94, 221, 47, 26, 159, 190, 119},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(456.789)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 7, 64, 124, 140, 159, 190, 118, 200, 180},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue([]byte{0, 1, 2, 3, 4})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 8, 1, 0, 1, 1, 2, 3, 4, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue("ABCD")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 8, 65, 66, 67, 68, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue("ab")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 8, 97, 98, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue("abc")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 8, 97, 98, 99, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue("abcd")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{1, 1, 8, 97, 98, 99, 100, 0},
			},
		})
}

func testParseKey(t *testing.T, row []sql.Value, colKeys []engine.ColumnKey) {
	t.Helper()

	key := bbolt.MakeKey(row, colKeys)
	dest := make([]sql.Value, len(row))
	ok := bbolt.ParseKey(key, colKeys, dest)
	if !ok {
		t.Errorf("ParseKey(%v, %v) failed", row, colKeys)
	}
	for _, ck := range colKeys {
		num := ck.Number()
		if !testutil.DeepEqual(dest[num], row[num], nil) {
			t.Errorf("ParseKey: at %d got %v want %v", num, dest[num], row[num])
		}
	}
}

func TestParseKey(t *testing.T) {

	row := []sql.Value{nil, sql.BoolValue(false), sql.BoolValue(true), sql.Int64Value(-456),
		sql.Int64Value(-123), sql.Int64Value(0), sql.Int64Value(123), sql.Int64Value(456),
		sql.Float64Value(-456.789), sql.Float64Value(-123.456), sql.Float64Value(0.0),
		sql.Float64Value(123.456), sql.Float64Value(456.789),
		sql.StringValue([]byte{0, 1, 2, 3, 4}), sql.StringValue("ABCD"), sql.StringValue("ab"),
		sql.StringValue("abc"), sql.StringValue("abcd")}

	for i := range row {
		testParseKey(t, row,
			[]engine.ColumnKey{
				engine.MakeColumnKey(i, false)})
	}

	for i := range row {
		for j := range row {
			if j == i {
				continue
			}

			testParseKey(t, row,
				[]engine.ColumnKey{
					engine.MakeColumnKey(i, false), engine.MakeColumnKey(j, false)})
		}
	}

	for i := range row {
		for j := range row {
			if j == i {
				continue
			}

			for k := range row {
				if k == i || k == j {
					continue
				}

				testParseKey(t, row,
					[]engine.ColumnKey{
						engine.MakeColumnKey(i, false), engine.MakeColumnKey(j, false),
						engine.MakeColumnKey(k, false)})
			}
		}
	}
	for i := range row {
		for j := range row {
			if j == i {
				continue
			}

			for k := range row {
				if k == i || k == j {
					continue
				}

				for l := range row {
					if l == i || l == j || l == k {
						continue
					}

					testParseKey(t, row,
						[]engine.ColumnKey{
							engine.MakeColumnKey(i, false), engine.MakeColumnKey(j, false),
							engine.MakeColumnKey(k, false), engine.MakeColumnKey(l, false)})
				}
			}
		}
	}
}
