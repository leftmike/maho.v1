package encoding_test

import (
	"bytes"
	"fmt"
	"math"
	"strings"
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
		ver          encoding.Version
	}{
		{
			tid: 1, iid: 2, s: "/1/2",
			key: []byte{0, 0, 0, 1, 0, 0, 0, 2},
			ver: 0x1234567890ABCDEF,
		},
		{
			tid: 1, iid: 2, s: "/1/2/NULL",
			vals: []sql.Value{nil},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 2,
				encoding.NullKeyTag},
			ver: 0x1234567890ABCDEF,
		},

		{
			tid: 1, iid: 3, s: "/1/3/true",
			vals: []sql.Value{sql.BoolValue(true)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 3,
				encoding.BoolKeyTag, 1},
			ver: 0x1234567890ABCDEF,
		},
		{
			tid: 1, iid: 4, s: "/1/4/false",
			vals: []sql.Value{sql.BoolValue(false)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 4,
				encoding.BoolKeyTag, 0},
			ver: 0x1234567890ABCDEF,
		},

		{
			tid: 1, iid: 5, s: "/1/5/0",
			vals: []sql.Value{sql.Int64Value(0)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 5,
				encoding.Int64NotNegKeyTag, 0, 0, 0, 0, 0, 0, 0, 0},
			ver: 0x1234567890ABCDEF,
		},
		{
			tid: 1, iid: 6, s: "/1/6/1311768467294899695",
			vals: []sql.Value{sql.Int64Value(0x1234567890ABCDEF)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 6,
				encoding.Int64NotNegKeyTag, 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF},
			ver: 0x1234567890ABCDEF,
		},
		{
			tid: 1, iid: 7, s: "/1/7/-98765",
			vals: []sql.Value{sql.Int64Value(-98765)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 7,
				encoding.Int64NegKeyTag, 255, 255, 255, 255, 255, 254, 126, 51},
			ver: 0x1234567890ABCDEF,
		},
		{
			tid: 1, iid: 7, s: "/1/7/-98765",
			vals: []sql.Value{sql.Int64Value(-98765)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 7,
				encoding.Int64NegKeyTag, 255, 255, 255, 255, 255, 254, 126, 51},
			ver: 0x1234567890ABCDEF,
		},

		{
			tid: 1, iid: 8, s: "/1/8/0",
			vals: []sql.Value{sql.Float64Value(0.0)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 8,
				encoding.Float64ZeroKeyTag},
			ver: 0x1234567890ABCDEF,
		},
		{
			tid: 1, iid: 9, s: "/1/9/NaN",
			vals: []sql.Value{sql.Float64Value(math.NaN())},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 9,
				encoding.Float64NaNKeyTag},
			noParseCheck: true,
			ver:          0x1234567890ABCDEF,
		},
		{
			tid: 1, iid: 10, s: "/1/10/1234.56789",
			vals: []sql.Value{sql.Float64Value(1234.56789)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 10,
				encoding.Float64PosKeyTag, 64, 147, 74, 69, 132, 244, 198, 231},
			ver: 0x1234567890ABCDEF,
		},
		{
			tid: 1, iid: 11, s: "/1/11/-0.000987654",
			vals: []sql.Value{sql.Float64Value(-0.000987654)},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 11,
				encoding.Float64NegKeyTag, 64, 175, 209, 122, 151, 177, 244, 20},
			ver: 0x1234567890ABCDEF,
		},

		{
			tid: 1, iid: 12, s: "/1/12/",
			vals: []sql.Value{sql.StringValue("")},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 12,
				encoding.StringKeyTag, 0},
			ver: 0x1234567890ABCDEF,
		},
		{
			tid: 1, iid: 13, s: "/1/13/abc",
			vals: []sql.Value{sql.StringValue("abc")},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 13,
				encoding.StringKeyTag, 'a', 'b', 'c', 0},
			ver: 0x1234567890ABCDEF,
		},
		{
			tid: 1, iid: 14, s: string([]byte{'/', '1', '/', '1', '4', '/', 0, 0, 1, 1, 0}),
			vals: []sql.Value{sql.StringValue([]byte{0, 0, 1, 1, 0})},
			key: []byte{0, 0, 0, 1, 0, 0, 0, 14,
				encoding.StringKeyTag, 1, 0, 1, 0, 1, 1, 1, 1, 1, 0, 0},
			ver: 0x1234567890ABCDEF,
		},

		{
			tid: 2, iid: 1, s: "/2/1/false/true",
			vals: []sql.Value{sql.BoolValue(false), sql.BoolValue(true)},
			key: []byte{0, 0, 0, 2, 0, 0, 0, 1,
				encoding.BoolKeyTag, 0,
				encoding.BoolKeyTag, 1},
			ver: 0x1234567890ABCDEF,
		},
		{
			tid: 2, iid: 2, s: "/2/2/123/true",
			vals: []sql.Value{sql.Int64Value(123), sql.BoolValue(true)},
			key: []byte{0, 0, 0, 2, 0, 0, 0, 2,
				encoding.Int64NotNegKeyTag, 0, 0, 0, 0, 0, 0, 0, 123,
				encoding.BoolKeyTag, 1},
			ver: 0x1234567890ABCDEF,
		},
		{
			tid: 2, iid: 3, s: "/2/3/123.456/true",
			vals: []sql.Value{sql.Float64Value(123.456), sql.BoolValue(true)},
			key: []byte{0, 0, 0, 2, 0, 0, 0, 3,
				encoding.Float64PosKeyTag, 64, 94, 221, 47, 26, 159, 190, 119,
				encoding.BoolKeyTag, 1},
			ver: 0x1234567890ABCDEF,
		},
		{
			tid: 2, iid: 4, s: "/2/4/abc/true",
			vals: []sql.Value{sql.StringValue("abc"), sql.BoolValue(true)},
			key: []byte{0, 0, 0, 2, 0, 0, 0, 4,
				encoding.StringKeyTag, 'a', 'b', 'c', 0,
				encoding.BoolKeyTag, 1},
			ver: 0x1234567890ABCDEF,
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
			ver: 0x1234567890ABCDEF,
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
		} else if !c.noParseCheck && !testutil.DeepEqual(vals, c.vals) {
			t.Errorf("ParseKey(%v, %d, %d): got %v want %v", key, c.tid, c.iid, vals, c.vals)
		}
		s := encoding.FormatKey(key)
		if s != c.s {
			t.Errorf("FormatKey(%d, %d): got %s want %s", c.tid, c.iid, s, c.s)
		}
	}

	for _, c := range cases {
		tid := c.tid + encoding.MinVersionedTID
		key := encoding.MakeVersionKey(tid, c.iid, c.ver, c.vals...)
		if !testutil.DeepEqual(key[4:len(key)-8], c.key[4:]) {
			t.Errorf("MakeVersionKey(%d, %d, %v): got %v want %v", tid, c.iid, c.vals,
				key[4:len(key)-8], c.key[4:])
		}
		vals, ver, ok := encoding.ParseVersionKey(key, tid, c.iid)
		if !ok {
			t.Errorf("ParseVersionKey(%v, %d, %d): failed", key, tid, c.iid)
		} else if !c.noParseCheck && !testutil.DeepEqual(vals, c.vals) {
			t.Errorf("ParseVersionKey(%v, %d, %d): got %v want %v", key, tid, c.iid, vals, c.vals)
		} else if ver != c.ver {
			t.Errorf("ParseVersionKey(%v, %d, %d): got %d want %d", key, tid, c.iid, ver, c.ver)
		}
		s := encoding.FormatKey(key)
		r := fmt.Sprintf("/%d/%s@%d", tid, strings.SplitN(c.s, "/", 3)[2], c.ver)
		if s != r {
			t.Errorf("FormatKey(%d, %d): got %s want %s", tid, c.iid, s, r)
		}
	}
}

func TestEncodedKeyOrdering(t *testing.T) {
	keys := []struct {
		tid  uint32
		iid  uint32
		vals []sql.Value
		s    string
		ver  encoding.Version
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

		{
			tid: 1, iid: 0, s: "/1/0",
		},
		{
			tid: 1, iid: 1, s: "/1/1",
		},
		{
			tid: 1, iid: 2, s: "/1/2",
		},
		{
			tid: 1, iid: 2, s: "/1/2/3",
			vals: []sql.Value{sql.Int64Value(3)},
		},
		{
			tid: 1, iid: 3, s: "/1/3",
		},
		{
			tid: 2, iid: 0, s: "/2/0",
		},

		{
			tid: 2, iid: 0, s: "/2/0/123/abc",
			vals: []sql.Value{sql.Int64Value(123), sql.StringValue("abc")},
		},
		{
			tid: 2, iid: 0, s: "/2/0/123/def",
			vals: []sql.Value{sql.Int64Value(123), sql.StringValue("def")},
		},
		{
			tid: 2, iid: 0, s: "/2/0/456/abc",
			vals: []sql.Value{sql.Int64Value(456), sql.StringValue("abc")},
		},

		{
			tid: 4096, iid: 1, s: "/4096/1/NULL@1",
			vals: []sql.Value{nil},
			ver:  1,
		},
		{
			tid: 4096, iid: 1, s: "/4096/1/false@1",
			vals: []sql.Value{sql.BoolValue(false)},
			ver:  1,
		},
		{
			tid: 4096, iid: 1, s: "/4096/1/true@1",
			vals: []sql.Value{sql.BoolValue(true)},
			ver:  1,
		},
		{
			tid: 4096, iid: 1, s: "/4096/1/@1",
			vals: []sql.Value{sql.StringValue("")},
			ver:  1,
		},
		{
			tid: 4096, iid: 2, s: "/4096/2//NULL@1",
			vals: []sql.Value{sql.StringValue(""), nil},
			ver:  1,
		},
		{
			tid: 4096, iid: 2, s: "/4096/2//@1",
			vals: []sql.Value{sql.StringValue(""), sql.StringValue("")},
			ver:  1,
		},

		{
			tid: 4096, iid: 3, s: "/4096/3/-3456@1",
			vals: []sql.Value{sql.Int64Value(-3456)},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/-345@1",
			vals: []sql.Value{sql.Int64Value(-345)},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/0@1",
			vals: []sql.Value{sql.Int64Value(0)},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/34@1",
			vals: []sql.Value{sql.Int64Value(34)},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/345@1",
			vals: []sql.Value{sql.Int64Value(345)},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/NaN@1",
			vals: []sql.Value{sql.Float64Value(math.NaN())},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/-123456.789@1",
			vals: []sql.Value{sql.Float64Value(-123456.789)},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/-0.0012345@1",
			vals: []sql.Value{sql.Float64Value(-0.0012345)},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/0@1",
			vals: []sql.Value{sql.Float64Value(0.0)},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/12.345@1",
			vals: []sql.Value{sql.Float64Value(12.345)},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/1.23456789e+12@1",
			vals: []sql.Value{sql.Float64Value(1234567890000)},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/@1",
			vals: []sql.Value{sql.StringValue("")},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/abc@1",
			vals: []sql.Value{sql.StringValue("abc")},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/abcd@1",
			vals: []sql.Value{sql.StringValue("abcd")},
			ver:  1,
		},
		{
			tid: 4096, iid: 3, s: "/4096/3/bc@1",
			vals: []sql.Value{sql.StringValue("bc")},
			ver:  1,
		},

		{
			tid: 4097, iid: 0, s: "/4097/0@1",
			ver: 1,
		},
		{
			tid: 4097, iid: 1, s: "/4097/1@1",
			ver: 1,
		},
		{
			tid: 4097, iid: 2, s: "/4097/2@1",
			ver: 1,
		},
		{
			tid: 4097, iid: 3, s: "/4097/3/4@1",
			vals: []sql.Value{sql.Int64Value(4)},
			ver:  1,
		},
		{
			tid: 4097, iid: 4, s: "/4097/4@1",
			ver: 1,
		},
		{
			tid: 4098, iid: 0, s: "/4098/0@1",
			ver: 1,
		},

		{
			tid: 4098, iid: 1, s: "/4098/1/123/abc@1",
			vals: []sql.Value{sql.Int64Value(123), sql.StringValue("abc")},
			ver:  1,
		},
		{
			tid: 4098, iid: 1, s: "/4098/1/123/def@1",
			vals: []sql.Value{sql.Int64Value(123), sql.StringValue("def")},
			ver:  1,
		},
		{
			tid: 4098, iid: 1, s: "/4098/1/456/abc@1",
			vals: []sql.Value{sql.Int64Value(456), sql.StringValue("abc")},
			ver:  1,
		},

		{
			tid: 4099, iid: 2, s: "/4099/2/1@18446744073709551615",
			vals: []sql.Value{sql.Int64Value(1)},
			ver:  0xFFFFFFFFFFFFFFFF,
		},
		{
			tid: 4099, iid: 2, s: "/4099/2/1@18446744073709551614",
			vals: []sql.Value{sql.Int64Value(1)},
			ver:  0xFFFFFFFFFFFFFFFE,
		},
		{
			tid: 4099, iid: 2, s: "/4099/2/1@18446744073709551600",
			vals: []sql.Value{sql.Int64Value(1)},
			ver:  0xFFFFFFFFFFFFFFF0,
		},
		{
			tid: 4099, iid: 2, s: "/4099/2/1@18446744069414584320",
			vals: []sql.Value{sql.Int64Value(1)},
			ver:  0xFFFFFFFF00000000,
		},
		{
			tid: 4099, iid: 2, s: "/4099/2/1@4294967295",
			vals: []sql.Value{sql.Int64Value(1)},
			ver:  0xFFFFFFFF,
		},
		{
			tid: 4099, iid: 2, s: "/4099/2/1@12345",
			vals: []sql.Value{sql.Int64Value(1)},
			ver:  12345,
		},
	}

	prevKey := encoding.MakeKey(0, 0)
	for _, k := range keys {
		var key []byte
		if k.ver > 0 {
			key = encoding.MakeVersionKey(k.tid, k.iid, k.ver, k.vals...)
		} else {
			key = encoding.MakeKey(k.tid, k.iid, k.vals...)
		}
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

func TestMakeKey(t *testing.T) {
	testPanic(t, "MakeKey",
		func() {
			encoding.MakeKey(9999, 1)
		})
}

func TestMakeVersionKey(t *testing.T) {
	testPanic(t, "MakeVersionKey",
		func() {
			encoding.MakeVersionKey(99, 1, 1111)
		})
}
