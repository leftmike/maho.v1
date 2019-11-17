package kvrows_test

import (
	"bytes"
	"math"
	"math/rand"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/leftmike/maho/engine"
	kvrows "github.com/leftmike/maho/engine/kvrows.old"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

type testCase struct {
	row     []sql.Value
	colKeys []engine.ColumnKey
	ret     []byte
}

func testProposalKey(t *testing.T, prevKey []byte, row []sql.Value, colKeys []engine.ColumnKey,
	tid, sid uint32) []byte {

	t.Helper()

	key := kvrows.MakeProposalKey(row, colKeys, tid, sid)
	if bytes.Compare(prevKey, key) >= 0 {
		t.Errorf("MakeProposalKey(%v, %v) keys not ordered correctly; %v and %v",
			row, colKeys, prevKey, key)
	}

	dest := make([]sql.Value, len(row))
	rettid, retsid, ok := kvrows.ParseProposalKey(key, colKeys, dest)
	if !ok {
		t.Errorf("ParseProposalKey(%v) failed", key)
	} else {
		if rettid != tid {
			t.Errorf("ParseProposalKey(%v) got %d for tid; want %d", key, rettid, tid)
		}
		if retsid != sid {
			t.Errorf("ParseProposalKey(%v) got %d for sid; want %d", key, retsid, sid)
		}
	}

	return key
}

func testDurableKey(t *testing.T, prevKey []byte, row []sql.Value, colKeys []engine.ColumnKey,
	ver uint64) []byte {

	t.Helper()

	key := kvrows.MakeDurableKey(row, colKeys, ver)
	if bytes.Compare(prevKey, key) >= 0 {
		t.Errorf("MakeDurableKey(%v, %v) keys not ordered correctly; %v and %v",
			row, colKeys, prevKey, key)
	}

	dest := make([]sql.Value, len(row))
	retver, ok := kvrows.ParseDurableKey(key, colKeys, dest)
	if !ok {
		t.Errorf("ParseDurableKey(%v) failed", key)
	} else {
		if retver != ver {
			t.Errorf("ParseDurableKey(%v) got %d for version; want %d", key, retver, ver)
		}
	}

	return key
}

func checkPrefixes(t *testing.T, prefixes [][]byte, i int, key []byte) {
	t.Helper()

	keyPrefix := kvrows.KeyPrefix(key)
	for j, prefix := range prefixes {
		if i == j {
			if !bytes.HasPrefix(key, prefix) {
				t.Errorf("MakePrefix(%d): key %v should have prefix %v", i, key, prefix)
			}
			if bytes.Compare(keyPrefix, prefix) != 0 {
				t.Errorf("KeyPrefix(%d): got %v want %v", i, keyPrefix, prefix)
			}
		} else {
			if bytes.HasPrefix(key, prefix) {
				t.Errorf("MakePrefix(%d, %d): key %v should not have prefix %v", i, j, key, prefix)
			}
			if bytes.Compare(keyPrefix, prefix) == 0 {
				t.Errorf("KeyPrefix(%d): key %v should not have prefix %v", i, key, prefix)
			}
		}
	}
}

func testMakeKey(t *testing.T, cases []testCase) {
	t.Helper()

	var prefixes [][]byte
	for _, c := range cases {
		prefixes = append(prefixes, kvrows.MakePrefix(c.row, c.colKeys))
	}

	var prevKey []byte
	for i, c := range cases {
		key := kvrows.MakeBareKey(c.row, c.colKeys)
		ret := append(c.ret, kvrows.BareKeyType)
		if bytes.Compare(key, ret) != 0 {
			t.Errorf("MakeBareKey(%d) got %v want %v", i, key, ret)
		}
		if bytes.Compare(prevKey, key) >= 0 {
			t.Errorf("MakeBareKey(%d) keys not ordered correctly; %v and %v", i, prevKey, key)
		}
		checkPrefixes(t, prefixes, i, key)
		prevKey = key

		tid := rand.Uint32()
		sid := uint32(rand.Intn(99999)) + 99
		prevKey = testProposalKey(t, prevKey, c.row, c.colKeys, tid, sid)
		prevKey = testProposalKey(t, prevKey, c.row, c.colKeys, tid, sid-1)
		prevKey = testProposalKey(t, prevKey, c.row, c.colKeys, tid, 1)
		prevKey = testProposalKey(t, prevKey, c.row, c.colKeys, tid, 0)

		ver := uint64(rand.Intn(99999)) + 99
		prevKey = testDurableKey(t, prevKey, c.row, c.colKeys, ver)
		prevKey = testDurableKey(t, prevKey, c.row, c.colKeys, ver-1)
		prevKey = testDurableKey(t, prevKey, c.row, c.colKeys, 1)
		prevKey = testDurableKey(t, prevKey, c.row, c.colKeys, 0)

	}
}

func TestMakeKey(t *testing.T) {
	testMakeKey(t,
		[]testCase{
			{
				row:     []sql.Value{nil},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 130},
			},
			{
				row:     []sql.Value{sql.BoolValue(false)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 131, 0},
			},
			{
				row:     []sql.Value{sql.BoolValue(true)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 131, 1},
			},
			{
				row:     []sql.Value{sql.Int64Value(-456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 140, 255, 255, 255, 255, 255, 255, 254, 56},
			},
			{
				row:     []sql.Value{sql.Int64Value(-123)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 140, 255, 255, 255, 255, 255, 255, 255, 133},
			},
			{
				row:     []sql.Value{sql.Int64Value(0)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 141, 0, 0, 0, 0, 0, 0, 0, 0},
			},
			{
				row:     []sql.Value{sql.Int64Value(123)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 141, 0, 0, 0, 0, 0, 0, 0, 123},
			},
			{
				row:     []sql.Value{sql.Int64Value(456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 141, 0, 0, 0, 0, 0, 0, 1, 200},
			},
			{
				row:     []sql.Value{sql.Float64Value(math.NaN())},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 150},
			},
			{
				row:     []sql.Value{sql.Float64Value(-456.789)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 151, 63, 131, 115, 96, 65, 137, 55, 75},
			},
			{
				row:     []sql.Value{sql.Float64Value(-123.456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 151, 63, 161, 34, 208, 229, 96, 65, 136},
			},
			{
				row:     []sql.Value{sql.Float64Value(0.0)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 152},
			},
			{
				row:     []sql.Value{sql.Float64Value(123.456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 153, 64, 94, 221, 47, 26, 159, 190, 119},
			},
			{
				row:     []sql.Value{sql.Float64Value(456.789)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 153, 64, 124, 140, 159, 190, 118, 200, 180},
			},
			{
				row:     []sql.Value{sql.StringValue([]byte{})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 160, 0},
			},
			{
				row:     []sql.Value{sql.StringValue([]byte{0})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 160, 1, 0, 0},
			},
			{
				row:     []sql.Value{sql.StringValue([]byte{0, 0})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 160, 1, 0, 1, 0, 0},
			},
			{
				row:     []sql.Value{sql.StringValue([]byte{0, 1, 2, 3, 4, 5, 6})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 160, 1, 0, 1, 1, 2, 3, 4, 5, 6, 0},
			},
			{
				row:     []sql.Value{sql.StringValue([]byte{1})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 160, 1, 1, 0},
			},
			{
				row:     []sql.Value{sql.StringValue([]byte{1, 1})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 160, 1, 1, 1, 1, 0},
			},
			{
				row:     []sql.Value{sql.StringValue([]byte{2})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 160, 2, 0},
			},
			{
				row:     []sql.Value{sql.StringValue([]byte{2, 2})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 160, 2, 2, 0},
			},
			{
				row:     []sql.Value{sql.StringValue([]byte{2, 3, 4, 5, 6})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 160, 2, 3, 4, 5, 6, 0},
			},
			{
				row:     []sql.Value{sql.StringValue("ABCD")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 160, 65, 66, 67, 68, 0},
			},
			{
				row:     []sql.Value{sql.StringValue("ab")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 160, 97, 98, 0},
			},
			{
				row:     []sql.Value{sql.StringValue("abc")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 160, 97, 98, 99, 0},
			},
			{
				row:     []sql.Value{sql.StringValue("abcd")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 160, 97, 98, 99, 100, 0},
			},
			{
				row:     []sql.Value{sql.BytesValue([]byte{})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 170, 0},
			},
			{
				row:     []sql.Value{sql.BytesValue([]byte{0})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 170, 1, 0, 0},
			},
			{
				row:     []sql.Value{sql.BytesValue([]byte{0, 0})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 170, 1, 0, 1, 0, 0},
			},
			{
				row:     []sql.Value{sql.BytesValue([]byte{0, 1, 2, 3, 4, 5, 6})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 170, 1, 0, 1, 1, 2, 3, 4, 5, 6, 0},
			},
			{
				row:     []sql.Value{sql.BytesValue([]byte{1})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 170, 1, 1, 0},
			},
			{
				row:     []sql.Value{sql.BytesValue([]byte{1, 1})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 170, 1, 1, 1, 1, 0},
			},
			{
				row:     []sql.Value{sql.BytesValue([]byte{2})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 170, 2, 0},
			},
			{
				row:     []sql.Value{sql.BytesValue([]byte{2, 2})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 170, 2, 2, 0},
			},
			{
				row:     []sql.Value{sql.BytesValue([]byte{2, 3, 4, 5, 6})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 170, 2, 3, 4, 5, 6, 0},
			},
			{
				row:     []sql.Value{sql.BytesValue("ABCD")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 170, 65, 66, 67, 68, 0},
			},
			{
				row:     []sql.Value{sql.BytesValue("ab")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 170, 97, 98, 0},
			},
			{
				row:     []sql.Value{sql.BytesValue("abc")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 170, 97, 98, 99, 0},
			},
			{
				row:     []sql.Value{sql.BytesValue("abcd")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 170, 97, 98, 99, 100, 0},
			},
			{
				row: []sql.Value{nil},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 130, 130},
			},
		})

	row := []sql.Value{nil, sql.BoolValue(false), sql.BoolValue(true), sql.Int64Value(-456),
		sql.Int64Value(-123), sql.Int64Value(0), sql.Int64Value(123), sql.Int64Value(456),
		sql.Float64Value(math.NaN()), sql.Float64Value(-456.789), sql.Float64Value(-123.456),
		sql.Float64Value(0.0), sql.Float64Value(123.456), sql.Float64Value(456.789),
		sql.StringValue([]byte{0, 1, 2, 3, 4}), sql.StringValue("ABCD"), sql.StringValue("ab"),
		sql.StringValue("abc"), sql.StringValue("abcd"), sql.BytesValue{0, 1, 2, 3},
		sql.BytesValue{1, 2, 3}, sql.BytesValue("abcd")}

	testMakeKey(t,
		[]testCase{
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false)},
				ret:     []byte{1, 130},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(1, false)},
				ret:     []byte{1, 131, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(2, false)},
				ret:     []byte{1, 131, 1},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(3, false)},
				ret:     []byte{1, 140, 255, 255, 255, 255, 255, 255, 254, 56},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(4, false)},
				ret:     []byte{1, 140, 255, 255, 255, 255, 255, 255, 255, 133},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(5, false)},
				ret:     []byte{1, 141, 0, 0, 0, 0, 0, 0, 0, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(6, false)},
				ret:     []byte{1, 141, 0, 0, 0, 0, 0, 0, 0, 123},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(7, false)},
				ret:     []byte{1, 141, 0, 0, 0, 0, 0, 0, 1, 200},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(8, false)},
				ret:     []byte{1, 150},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(9, false)},
				ret:     []byte{1, 151, 63, 131, 115, 96, 65, 137, 55, 75},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(10, false)},
				ret:     []byte{1, 151, 63, 161, 34, 208, 229, 96, 65, 136},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(11, false)},
				ret:     []byte{1, 152},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(12, false)},
				ret:     []byte{1, 153, 64, 94, 221, 47, 26, 159, 190, 119},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(13, false)},
				ret:     []byte{1, 153, 64, 124, 140, 159, 190, 118, 200, 180},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(14, false)},
				ret:     []byte{1, 160, 1, 0, 1, 1, 2, 3, 4, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(15, false)},
				ret:     []byte{1, 160, 65, 66, 67, 68, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(16, false)},
				ret:     []byte{1, 160, 97, 98, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(17, false)},
				ret:     []byte{1, 160, 97, 98, 99, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(18, false)},
				ret:     []byte{1, 160, 97, 98, 99, 100, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(19, false)},
				ret:     []byte{1, 170, 1, 0, 1, 1, 2, 3, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(20, false)},
				ret:     []byte{1, 170, 1, 1, 2, 3, 0},
			},
			{
				row:     row,
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(21, false)},
				ret:     []byte{1, 170, 97, 98, 99, 100, 0},
			},
		})

	testMakeKey(t,
		[]testCase{
			{
				row: []sql.Value{sql.BoolValue(true), nil},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 130},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BoolValue(false)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 131, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BoolValue(true)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 131, 1},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(-456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 140, 255, 255, 255, 255, 255, 255, 254, 56},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(-123)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 140, 255, 255, 255, 255, 255, 255, 255, 133},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(0)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 141, 0, 0, 0, 0, 0, 0, 0, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(123)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 141, 0, 0, 0, 0, 0, 0, 0, 123},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 141, 0, 0, 0, 0, 0, 0, 1, 200},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(math.NaN())},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 150},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(-456.789)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 151, 63, 131, 115, 96, 65, 137, 55, 75},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(-123.456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 151, 63, 161, 34, 208, 229, 96, 65, 136},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(0.0)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 152},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(123.456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 153, 64, 94, 221, 47, 26, 159, 190, 119},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(456.789)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 153, 64, 124, 140, 159, 190, 118, 200, 180},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue([]byte{0, 1, 2, 3, 4})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 160, 1, 0, 1, 1, 2, 3, 4, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue("ABCD")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 160, 65, 66, 67, 68, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue("ab")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 160, 97, 98, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue("abc")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 160, 97, 98, 99, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue("abcd")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 160, 97, 98, 99, 100, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BytesValue([]byte{0, 1, 2, 3, 4})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 170, 1, 0, 1, 1, 2, 3, 4, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BytesValue("ABCD")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 170, 65, 66, 67, 68, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BytesValue("ab")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 170, 97, 98, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BytesValue("abc")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 170, 97, 98, 99, 0},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BytesValue("abcd")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, false)},
				ret: []byte{2, 131, 1, 170, 97, 98, 99, 100, 0},
			},
		})

	testMakeKey(t,
		[]testCase{
			{
				row: []sql.Value{sql.BoolValue(true), sql.BytesValue("abcd")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 85, 158, 157, 156, 155, 255},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BytesValue("abc")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 85, 158, 157, 156, 255},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BytesValue("ab")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 85, 158, 157, 255},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BytesValue("ABCD")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 85, 190, 189, 188, 187, 255},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BytesValue([]byte{0, 1, 2, 3, 4})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 85, 254, 255, 254, 254, 253, 252, 251, 255},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue("abcd")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 95, 158, 157, 156, 155, 255},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue("abc")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 95, 158, 157, 156, 255},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue("ab")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 95, 158, 157, 255},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue("ABCD")},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 95, 190, 189, 188, 187, 255},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.StringValue([]byte{0, 1, 2, 3, 4})},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 95, 254, 255, 254, 254, 253, 252, 251, 255},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(456.789)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 102, 191, 131, 115, 96, 65, 137, 55, 75},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(123.456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 102, 191, 161, 34, 208, 229, 96, 65, 136},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(0.0)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 103},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(-123.456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 104, 192, 94, 221, 47, 26, 159, 190, 119},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(-456.789)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 104, 192, 124, 140, 159, 190, 118, 200, 180},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Float64Value(math.NaN())},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 105},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 114, 255, 255, 255, 255, 255, 255, 254, 55},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(123)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 114, 255, 255, 255, 255, 255, 255, 255, 132},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(0)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 114, 255, 255, 255, 255, 255, 255, 255, 255},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(-123)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 115, 0, 0, 0, 0, 0, 0, 0, 122},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.Int64Value(-456)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 115, 0, 0, 0, 0, 0, 0, 1, 199},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BoolValue(true)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 124, 254},
			},
			{
				row: []sql.Value{sql.BoolValue(true), sql.BoolValue(false)},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 124, 255},
			},
			{
				row: []sql.Value{sql.BoolValue(true), nil},
				colKeys: []engine.ColumnKey{engine.MakeColumnKey(0, false),
					engine.MakeColumnKey(1, true)},
				ret: []byte{2, 131, 1, 125},
			},
		})
}

func testParseKey(t *testing.T, row []sql.Value, colKeys []engine.ColumnKey) {
	t.Helper()

	key := kvrows.MakeBareKey(row, colKeys)
	dest := make([]sql.Value, len(row))
	ok := kvrows.ParseBareKey(key, colKeys, dest)
	if !ok {
		t.Errorf("ParseKey(%v, %v) failed", row, colKeys)
	}
	for _, ck := range colKeys {
		num := ck.Number()
		if !testutil.DeepEqual(dest[num], row[num]) {
			t.Errorf("ParseKey: at %d got %v want %v", num, dest[num], row[num])
		}
	}
}

func testParseKeyReverse(t *testing.T, row []sql.Value, reverse bool) {
	t.Helper()

	for i := range row {
		testParseKey(t, row,
			[]engine.ColumnKey{
				engine.MakeColumnKey(i, reverse)})
	}

	for i := range row {
		for j := range row {
			if j == i {
				continue
			}

			testParseKey(t, row,
				[]engine.ColumnKey{
					engine.MakeColumnKey(i, reverse), engine.MakeColumnKey(j, reverse)})
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
						engine.MakeColumnKey(i, reverse), engine.MakeColumnKey(j, reverse),
						engine.MakeColumnKey(k, reverse)})
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
							engine.MakeColumnKey(i, reverse), engine.MakeColumnKey(j, reverse),
							engine.MakeColumnKey(k, reverse), engine.MakeColumnKey(l, reverse)})
				}
			}
		}
	}
}

func TestParseKey(t *testing.T) {
	row := []sql.Value{nil, sql.BoolValue(false), sql.BoolValue(true), sql.Int64Value(-456),
		sql.Int64Value(-123), sql.Int64Value(0), sql.Int64Value(123), sql.Int64Value(456),
		sql.Float64Value(-456.789), sql.Float64Value(-123.456), sql.Float64Value(0.0),
		sql.Float64Value(123.456), sql.Float64Value(456.789),
		sql.StringValue([]byte{0, 1, 2, 3, 4}), sql.StringValue("ABCD"), sql.StringValue("ab"),
		sql.StringValue("abc"), sql.StringValue("abcd"), sql.BytesValue{0, 1, 2, 3},
		sql.BytesValue{0xFF, 0, 0, 0xFF}, sql.BytesValue{0xFF, 1, 1, 1, 1}}

	testParseKeyReverse(t, row, false)
	testParseKeyReverse(t, row, true)
}

func TestEncodeVarint(t *testing.T) {
	numbers := []uint64{
		0,
		1,
		125,
		126,
		127,
		0xFF,
		0x100,
		0xFFF,
		0x1000,
		0x7F7F,
		1234567890,
		math.MaxUint32,
		math.MaxUint64,
	}

	for _, n := range numbers {
		buf := kvrows.EncodeVarint(nil, n)
		pbuf := proto.EncodeVarint(n)
		if !testutil.DeepEqual(buf, pbuf) {
			t.Errorf("EncodeVarint(%d): got %v want %v", n, buf, pbuf)
		}
		ret, r, ok := kvrows.DecodeVarint(buf)
		if !ok {
			t.Errorf("DecodeVarint(%v) failed", buf)
		} else if len(ret) != 0 {
			t.Errorf("DecodeVarint(%v): got %v want []", buf, ret)
		} else if n != r {
			t.Errorf("DecodeVarint(%v): got %d want %d", buf, r, n)
		}
	}
}

func TestEncodeZigzag64(t *testing.T) {
	numbers := []int64{
		0,
		1,
		125,
		126,
		127,
		128,
		129,
		0xFF,
		0x100,
		0xFFF,
		0x1000,
		0x7F7F,
		1234567890,
		10000000000,
		math.MaxInt32,
		math.MaxInt64,
		math.MinInt32,
		math.MinInt64,
		-987654321,
		-1000000000,
		-125,
		-126,
		-127,
		-128,
		-129,
		-0xFF,
	}

	for _, n := range numbers {
		buf := kvrows.EncodeZigzag64(nil, n)
		enc := proto.NewBuffer(nil)
		err := enc.EncodeZigzag64(uint64(n))
		if err != nil {
			t.Errorf("proto.EncodeZigzag64(%d) failed with %s", n, err)
		} else {
			pbuf := enc.Bytes()
			if !testutil.DeepEqual(buf, pbuf) {
				t.Errorf("EncodeZigzag64(%d): got %v want %v", n, buf, pbuf)
			}
		}
		ret, r, ok := kvrows.DecodeZigzag64(buf)
		if !ok {
			t.Errorf("DecodeZigzag64(%v) failed", buf)
		} else if len(ret) != 0 {
			t.Errorf("DecodeZigzag64(%v): got %v want []", buf, ret)
		} else if n != r {
			t.Errorf("DecodeZigzag64(%v): got %d want %d", buf, r, n)
		}
	}
}

func TestMakeParseValues(t *testing.T) {
	cases := []struct {
		row []sql.Value
		s   string
	}{
		{
			row: []sql.Value{sql.BoolValue(true)},
			s:   "true",
		},
		{
			row: []sql.Value{sql.Int64Value(345)},
			s:   "345",
		},
		{
			row: []sql.Value{sql.Float64Value(987.6543)},
			s:   "987.6543",
		},
		{
			row: []sql.Value{sql.StringValue("abcdefghijklmnopqrstuvwxyz")},
			s:   "'abcdefghijklmnopqrstuvwxyz'",
		},
		{
			row: []sql.Value{sql.BoolValue(true), sql.Int64Value(345), sql.Float64Value(987.6543),
				sql.StringValue("abcdefghijklmnopqrstuvwxyz")},
			s: "true, 345, 987.6543, 'abcdefghijklmnopqrstuvwxyz'",
		},
		{
			row: []sql.Value{sql.BoolValue(true), nil, sql.Int64Value(345)},
			s:   "true, NULL, 345",
		},
		{
			row: []sql.Value{nil, nil, nil, sql.StringValue("ABCDEFG")},
			s:   "NULL, NULL, NULL, 'ABCDEFG'",
		},
		{
			row: []sql.Value{sql.Int64Value(1234567890), sql.StringValue(""), sql.BoolValue(true)},
			s:   "1234567890, '', true",
		},
		{
			row: []sql.Value{sql.Int64Value(123), sql.StringValue(""),
				sql.BytesValue{0xFF, 1, 2, 3}},
			s: "123, '', x'ff010203'",
		},
	}

	for _, c := range cases {
		buf := kvrows.MakeRowValue(c.row)
		if !kvrows.IsRowValue(buf) {
			t.Errorf("IsRowValue(%s) failed", c.s)
		}
		if kvrows.IsTombstoneValue(buf) {
			t.Errorf("IsTombstoneValue(%s) succeeded", c.s)
		}
		if kvrows.IsGobValue(buf) {
			t.Errorf("IsGobValue(%s) succeeded", c.s)
		}
		dest := make([]sql.Value, len(c.row))
		ok := kvrows.ParseRowValue(buf, dest)
		if !ok {
			t.Errorf("ParseRowValue(%s) failed", c.s)
		} else if !testutil.DeepEqual(c.row, dest) {
			t.Errorf("ParseRowValue(%s) got %v want %v", c.s, dest, c.row)
		}

		var s string
		for num, val := range dest {
			if num > 0 {
				s += ", "
			}
			s += sql.Format(val)
		}
		if s != c.s {
			t.Errorf("ParseRowValue: got %s want %s", s, c.s)
		}
	}

	if kvrows.IsRowValue(kvrows.MakeTombstoneValue()) {
		t.Errorf("IsRowValue(MakeTombstoneValue()) succeeded")
	}
	if kvrows.IsGobValue(kvrows.MakeTombstoneValue()) {
		t.Errorf("IsGobValue(MakeTombstoneValue()) succeeded")
	}
	if !kvrows.IsTombstoneValue(kvrows.MakeTombstoneValue()) {
		t.Errorf("IsTombstoneValue(MakeTombstoneValue()) failed")
	}
}

func TestGobValues(t *testing.T) {
	type testValue struct {
		Bool   bool
		Int    int
		String string
	}
	value := testValue{true, 12345, "string value"}

	buf, err := kvrows.MakeGobValue(&value)
	if err != nil {
		t.Errorf("MakeGobValue() failed with %s", err)
	}
	if !kvrows.IsGobValue(buf) {
		t.Errorf("IsGobValue(MakeGobValue()) failed")
	}
	if kvrows.IsRowValue(buf) {
		t.Errorf("IsRowValue(MakeGobValue()) succeeded")
	}
	if kvrows.IsTombstoneValue(buf) {
		t.Errorf("IsTombstoneValue(MakeGobValue()) succeeded")
	}

	var result testValue
	ok := kvrows.ParseGobValue(buf, &result)
	if !ok {
		t.Errorf("ParseGobValue() failed")
	} else if !testutil.DeepEqual(&result, &value) {
		t.Errorf("ParseGobValue() got %v want %v", &result, &value)
	}
}
