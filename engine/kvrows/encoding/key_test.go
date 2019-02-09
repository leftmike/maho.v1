package encoding_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/leftmike/maho/engine/kvrows/encoding"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

func TestEncodingKeys(t *testing.T) {
	cases := []struct {
		tid          uint32
		iid          uint32
		vals         []sql.Value
		s            string
		key          []byte
		noParseCheck bool // for NaN
	}{
		{
			tid: 1, iid: 2, s: "/1/2",
			key: []byte{0, 0, 0, 1, 0, 0, 0, 2},
		},
		{
			tid: 1, iid: 2, s: "/1/2/NULL",
			vals: []sql.Value{nil},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 2,
				encoding.NullKeyTag},
		},

		{
			tid: 1, iid: 3, s: "/1/3/true",
			vals: []sql.Value{sql.BoolValue(true)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 3,
				encoding.BoolKeyTag, 1},
		},
		{
			tid: 1, iid: 4, s: "/1/4/false",
			vals: []sql.Value{sql.BoolValue(false)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 4,
				encoding.BoolKeyTag, 0},
		},

		{
			tid: 1, iid: 5, s: "/1/5/0",
			vals: []sql.Value{sql.Int64Value(0)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 5,
				encoding.Int64NotNegKeyTag, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			tid: 1, iid: 6, s: "/1/6/1311768467294899695",
			vals: []sql.Value{sql.Int64Value(0x1234567890ABCDEF)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 6,
				encoding.Int64NotNegKeyTag, 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF},
		},
		{
			tid: 1, iid: 7, s: "/1/7/-98765",
			vals: []sql.Value{sql.Int64Value(-98765)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 7,
				encoding.Int64NegKeyTag, 255, 255, 255, 255, 255, 254, 126, 51},
		},
		{
			tid: 1, iid: 7, s: "/1/7/-98765",
			vals: []sql.Value{sql.Int64Value(-98765)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 7,
				encoding.Int64NegKeyTag, 255, 255, 255, 255, 255, 254, 126, 51},
		},

		{
			tid: 1, iid: 8, s: "/1/8/0",
			vals: []sql.Value{sql.Float64Value(0.0)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 8,
				encoding.Float64ZeroKeyTag},
		},
		{
			tid: 1, iid: 9, s: "/1/9/NaN",
			vals: []sql.Value{sql.Float64Value(math.NaN())},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 9,
				encoding.Float64NaNKeyTag},
			noParseCheck: true,
		},
		{
			tid: 1, iid: 10, s: "/1/10/1234.56789",
			vals: []sql.Value{sql.Float64Value(1234.56789)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 10,
				encoding.Float64PosKeyTag, 64, 147, 74, 69, 132, 244, 198, 231},
		},
		{
			tid: 1, iid: 11, s: "/1/11/-0.000987654",
			vals: []sql.Value{sql.Float64Value(-0.000987654)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 11,
				encoding.Float64NegKeyTag, 64, 175, 209, 122, 151, 177, 244, 20},
		},

		{
			tid: 1, iid: 12, s: "/1/12/",
			vals: []sql.Value{sql.StringValue("")},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 12,
				encoding.StringKeyTag, 0},
		},
		{
			tid: 1, iid: 13, s: "/1/13/abc",
			vals: []sql.Value{sql.StringValue("abc")},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 13,
				encoding.StringKeyTag, 'a', 'b', 'c', 0},
		},
		{
			tid: 1, iid: 14, s: string([]byte{'/', '1', '/', '1', '4', '/', 0, 0, 1, 1, 0}),
			vals: []sql.Value{sql.StringValue([]byte{0, 0, 1, 1, 0})},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 14,
				encoding.StringKeyTag, 1, 0, 1, 0, 1, 1, 1, 1, 1, 0, 0},
		},

		{
			tid: 2, iid: 1, s: "/2/1/false/true",
			vals: []sql.Value{sql.BoolValue(false), sql.BoolValue(true)},
			key: []byte{0, 0, 0, 2, 0, 0, 0, 1,
				encoding.BoolKeyTag, 0,
				encoding.BoolKeyTag, 1},
		},
		{
			tid: 2, iid: 2, s: "/2/2/123/true",
			vals: []sql.Value{sql.Int64Value(123), sql.BoolValue(true)},
			key: []byte{0, 0, 0, 2, 0, 0, 0, 2,
				encoding.Int64NotNegKeyTag, 0, 0, 0, 0, 0, 0, 0, 123,
				encoding.BoolKeyTag, 1},
		},
		{
			tid: 2, iid: 3, s: "/2/3/123.456/true",
			vals: []sql.Value{sql.Float64Value(123.456), sql.BoolValue(true)},
			key: []byte{0, 0, 0, 2, 0, 0, 0, 3,
				encoding.Float64PosKeyTag, 64, 94, 221, 47, 26, 159, 190, 119,
				encoding.BoolKeyTag, 1},
		},
		{
			tid: 2, iid: 4, s: "/2/4/abc/true",
			vals: []sql.Value{sql.StringValue("abc"), sql.BoolValue(true)},
			key: []byte{0, 0, 0, 2, 0, 0, 0, 4,
				encoding.StringKeyTag, 'a', 'b', 'c', 0,
				encoding.BoolKeyTag, 1},
		},

		{
			tid: 3, iid: 1000, s: "/3/1000/false/abc/123/def/456/true/ghi/NULL",
			vals: []sql.Value{sql.BoolValue(false), sql.StringValue("abc"), sql.Int64Value(123),
				sql.StringValue("def"), sql.Int64Value(456), sql.BoolValue(true),
				sql.StringValue("ghi"), nil},
			key: []byte{0, 0, 0, 3, 0, 0, 3, 232,
				encoding.BoolKeyTag, 0,
				encoding.StringKeyTag, 'a', 'b', 'c', 0,
				encoding.Int64NotNegKeyTag, 0, 0, 0, 0, 0, 0, 0, 123,
				encoding.StringKeyTag, 'd', 'e', 'f', 0,
				encoding.Int64NotNegKeyTag, 0, 0, 0, 0, 0, 0, 1, 200,
				encoding.BoolKeyTag, 1,
				encoding.StringKeyTag, 'g', 'h', 'i', 0,
				encoding.NullKeyTag},
		},
	}

	for _, c := range cases {
		key := encoding.MakeKey(c.tid, c.iid, c.vals...)
		if !testutil.DeepEqual(key, c.key) {
			t.Errorf("MakeKey(%d, %d, %v): got %v want %v", c.tid, c.iid, c.vals, key, c.key)
		}
		vals, ok := encoding.ParseKey(key, c.tid, c.iid)
		if !ok {
			t.Errorf("ParseKey(%v, %d, %d): failed", key, c.tid, c.iid)
		}
		if !c.noParseCheck && !testutil.DeepEqual(vals, c.vals) {
			t.Errorf("ParseKey(%v, %d, %d): got %v want %v", key, c.tid, c.iid, vals, c.vals)
		}
		s := encoding.FormatKey(key)
		if s != c.s {
			t.Errorf("FormatKey(%d, %d): got %s want %s", c.tid, c.iid, s, c.s)
		}
	}
}

func TestEncodedKeyOrdering(t *testing.T) {
	keys := []struct {
		tid  uint32
		iid  uint32
		vals []sql.Value
		s    string
	}{
		{
			tid: 0, iid: 1, s: "/0/1",
		},
		{
			tid: 0, iid: 1, s: "/0/1/NULL",
			vals: []sql.Value{nil},
		},
		{
			tid: 0, iid: 1, s: "/0/1/false",
			vals: []sql.Value{sql.BoolValue(false)},
		},
		{
			tid: 0, iid: 1, s: "/0/1/true",
			vals: []sql.Value{sql.BoolValue(true)},
		},
		{
			tid: 0, iid: 1, s: "/0/1/",
			vals: []sql.Value{sql.StringValue("")},
		},
		{
			tid: 0, iid: 1, s: "/0/1//NULL",
			vals: []sql.Value{sql.StringValue(""), nil},
		},
		{
			tid: 0, iid: 1, s: "/0/1//",
			vals: []sql.Value{sql.StringValue(""), sql.StringValue("")},
		},

		{
			tid: 0, iid: 2, s: "/0/2/-3456",
			vals: []sql.Value{sql.Int64Value(-3456)},
		},
		{
			tid: 0, iid: 2, s: "/0/2/-345",
			vals: []sql.Value{sql.Int64Value(-345)},
		},
		{
			tid: 0, iid: 2, s: "/0/2/0",
			vals: []sql.Value{sql.Int64Value(0)},
		},
		{
			tid: 0, iid: 2, s: "/0/2/34",
			vals: []sql.Value{sql.Int64Value(34)},
		},
		{
			tid: 0, iid: 2, s: "/0/2/345",
			vals: []sql.Value{sql.Int64Value(345)},
		},
		{
			tid: 0, iid: 2, s: "/0/2/NaN",
			vals: []sql.Value{sql.Float64Value(math.NaN())},
		},
		{
			tid: 0, iid: 2, s: "/0/2/-123456.789",
			vals: []sql.Value{sql.Float64Value(-123456.789)},
		},
		{
			tid: 0, iid: 2, s: "/0/2/-0.0012345",
			vals: []sql.Value{sql.Float64Value(-0.0012345)},
		},
		{
			tid: 0, iid: 2, s: "/0/2/0",
			vals: []sql.Value{sql.Float64Value(0.0)},
		},
		{
			tid: 0, iid: 2, s: "/0/2/12.345",
			vals: []sql.Value{sql.Float64Value(12.345)},
		},
		{
			tid: 0, iid: 2, s: "/0/2/1.23456789e+12",
			vals: []sql.Value{sql.Float64Value(1234567890000)},
		},
		{
			tid: 0, iid: 2, s: "/0/2/",
			vals: []sql.Value{sql.StringValue("")},
		},
		{
			tid: 0, iid: 2, s: "/0/2/abc",
			vals: []sql.Value{sql.StringValue("abc")},
		},
		{
			tid: 0, iid: 2, s: "/0/2/abcd",
			vals: []sql.Value{sql.StringValue("abcd")},
		},
		{
			tid: 0, iid: 2, s: "/0/2/bc",
			vals: []sql.Value{sql.StringValue("bc")},
		},

		// XXX: test ordering with multiple keys
		// XXX: test ordering with different tid and iid
	}

	prevKey := encoding.MakeKey(0, 0)
	for _, k := range keys {
		key := encoding.MakeKey(k.tid, k.iid, k.vals...)
		s := encoding.FormatKey(key)
		if s != k.s {
			t.Errorf("FormatKey(%d, %d): got %s want %s", k.tid, k.iid, s, k.s)
		}
		if bytes.Compare(prevKey, key) != -1 {
			t.Errorf("%s not less than %s", encoding.FormatKey(prevKey), k.s)
		}
		prevKey = key
	}
}
