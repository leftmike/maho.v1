package encoding_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/leftmike/maho/engine/kvrows/encoding"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

func testKeys(t *testing.T, keyType encoding.KeyType,
	makeKey func(idx int, s string, tid, iid uint32, vals []sql.Value) ([]byte, string)) {

	cases := []struct {
		tid, iid uint32
		vals     []sql.Value
		s        string
	}{
		{
			tid: 1, iid: 2, s: "/1/2",
		},
		{
			tid: 1, iid: 2, s: "/1/2/NULL",
			vals: []sql.Value{nil},
		},

		{
			tid: 1, iid: 3, s: "/1/3/true",
			vals: []sql.Value{sql.BoolValue(true)},
		},
		{
			tid: 1, iid: 4, s: "/1/4/false",
			vals: []sql.Value{sql.BoolValue(false)},
		},

		{
			tid: 1, iid: 5, s: "/1/5/0",
			vals: []sql.Value{sql.Int64Value(0)},
		},
		{
			tid: 1, iid: 6, s: "/1/6/1311768467294899695",
			vals: []sql.Value{sql.Int64Value(0x1234567890ABCDEF)},
		},
		{
			tid: 1, iid: 7, s: "/1/7/-98765",
			vals: []sql.Value{sql.Int64Value(-98765)},
		},
		{
			tid: 1, iid: 7, s: "/1/7/-98765",
			vals: []sql.Value{sql.Int64Value(-98765)},
		},

		{
			tid: 1, iid: 8, s: "/1/8/0",
			vals: []sql.Value{sql.Float64Value(0.0)},
		},
		{
			tid: 1, iid: 10, s: "/1/10/1234.56789",
			vals: []sql.Value{sql.Float64Value(1234.56789)},
		},
		{
			tid: 1, iid: 11, s: "/1/11/-0.000987654",
			vals: []sql.Value{sql.Float64Value(-0.000987654)},
		},

		{
			tid: 1, iid: 12, s: "/1/12/",
			vals: []sql.Value{sql.StringValue("")},
		},
		{
			tid: 1, iid: 13, s: "/1/13/abc",
			vals: []sql.Value{sql.StringValue("abc")},
		},
		{
			tid: 1, iid: 14, s: string([]byte{'/', '1', '/', '1', '4', '/', 0, 0, 1, 1, 0}),
			vals: []sql.Value{sql.StringValue([]byte{0, 0, 1, 1, 0})},
		},

		{
			tid: 2, iid: 1, s: "/2/1/false/true",
			vals: []sql.Value{sql.BoolValue(false), sql.BoolValue(true)},
		},
		{
			tid: 2, iid: 2, s: "/2/2/123/true",
			vals: []sql.Value{sql.Int64Value(123), sql.BoolValue(true)},
		},
		{
			tid: 2, iid: 3, s: "/2/3/123.456/true",
			vals: []sql.Value{sql.Float64Value(123.456), sql.BoolValue(true)},
		},
		{
			tid: 2, iid: 4, s: "/2/4/abc/true",
			vals: []sql.Value{sql.StringValue("abc"), sql.BoolValue(true)},
		},

		{
			tid: 3, iid: 1000, s: "/3/1000/false/abc/123/def/456/true/ghi/NULL",
			vals: []sql.Value{sql.BoolValue(false), sql.StringValue("abc"), sql.Int64Value(123),
				sql.StringValue("def"), sql.Int64Value(456), sql.BoolValue(true),
				sql.StringValue("ghi"), nil},
		},
	}

	for idx, c := range cases {
		key, ret := makeKey(idx, c.s, c.tid, c.iid, c.vals)
		if encoding.GetKeyType(key) != keyType {
			t.Errorf("MakeBareKey(%d, %d, %v): key type: got %d want %d", c.tid, c.iid, c.vals,
				encoding.GetKeyType(key), keyType)
		}
		tid, iid, vals, kt, ok := encoding.ParseKey(key)
		if !ok {
			t.Errorf("ParseKey(%s): failed", c.s)
		} else if tid != c.tid {
			t.Errorf("ParseKey(%s): tid: got %d want %d", c.s, tid, c.tid)
		} else if iid != c.iid {
			t.Errorf("ParseKey(%s): iid: got %d want %d", c.s, iid, c.iid)
		} else if kt != keyType {
			t.Errorf("ParseKey(%s): key type: got %d want %d", c.s, kt, keyType)
		} else if !testutil.DeepEqual(vals, c.vals) {
			t.Errorf("ParseKey(%s): vals: got %v want %v", c.s, vals, c.vals)
		}
		s := encoding.FormatKey(key)
		if s != ret {
			t.Errorf("FormatKey(%s): got %s", ret, s)
		}
	}
}

func TestBareKeys(t *testing.T) {
	testKeys(t, encoding.BareKeyType,
		func(idx int, s string, tid, iid uint32, vals []sql.Value) ([]byte, string) {
			return encoding.MakeBareKey(tid, iid, vals), s
		})
}

func TestProposalKeys(t *testing.T) {
	testKeys(t, encoding.ProposalKeyType,
		func(idx int, s string, tid, iid uint32, vals []sql.Value) ([]byte, string) {
			return encoding.MakeProposalKey(tid, iid, vals), s + "@proposal"
		})
}

func TestProposedWriteKeys(t *testing.T) {
	cases := []struct {
		stmtid uint32
		s      string
	}{
		{stmtid: 0, s: "@stmt(0)"},
		{stmtid: 12345678, s: "@stmt(12345678)"},
		{stmtid: 333, s: "@stmt(333)"},
	}

	testKeys(t, encoding.ProposedWriteKeyType,
		func(idx int, s string, tid, iid uint32, vals []sql.Value) ([]byte, string) {
			c := cases[idx%len(cases)]
			return encoding.MakeProposedWriteKey(tid, iid, vals, c.stmtid), s + c.s
		})
}

func TestTransactionKeys(t *testing.T) {
	cases := []struct {
		txid uint32
		s    string
	}{
		{txid: 0, s: "@txid(0)"},
		{txid: 12345678, s: "@txid(12345678)"},
		{txid: 333, s: "@txid(333)"},
	}

	testKeys(t, encoding.TransactionKeyType,
		func(idx int, s string, tid, iid uint32, vals []sql.Value) ([]byte, string) {
			c := cases[idx%len(cases)]
			return encoding.MakeTransactionKey(tid, iid, vals, c.txid), s + c.s
		})
}

func TestVersionKeys(t *testing.T) {
	cases := []struct {
		ver encoding.Version
		s   string
	}{
		{ver: 0, s: "@0"},
		{ver: 12345678, s: "@12345678"},
		{ver: 333, s: "@333"},
		{ver: 987654321098765, s: "@987654321098765"},
	}

	testKeys(t, encoding.VersionKeyType,
		func(idx int, s string, tid, iid uint32, vals []sql.Value) ([]byte, string) {
			c := cases[idx%len(cases)]
			return encoding.MakeVersionKey(tid, iid, vals, c.ver), s + c.s
		})
}

func TestNaNKey(t *testing.T) {
	cases := []struct {
		tid, iid uint32
		vals     []sql.Value
		s        string
	}{
		{
			tid: 1, iid: 9, s: "/1/9/NaN",
			vals: []sql.Value{sql.Float64Value(math.NaN())},
		},
	}

	for _, c := range cases {
		key := encoding.MakeBareKey(c.tid, c.iid, c.vals)
		if encoding.GetKeyType(key) != encoding.BareKeyType {
			t.Errorf("MakeBareKey(%d, %d, %v): got %d key type", c.tid, c.iid, c.vals,
				encoding.GetKeyType(key))
		}
		tid, iid, _, kt, ok := encoding.ParseKey(key)
		if !ok {
			t.Errorf("ParseKey(%s): failed", c.s)
		} else if tid != c.tid {
			t.Errorf("ParseKey(%s): tid: got %d want %d", c.s, tid, c.tid)
		} else if iid != c.iid {
			t.Errorf("ParseKey(%s): iid: got %d want %d", c.s, iid, c.iid)
		} else if kt != encoding.BareKeyType {
			t.Errorf("ParseKey(%s): key type: got %d", c.s, kt)
		}
		s := encoding.FormatKey(key)
		if s != c.s {
			t.Errorf("FormatKey(%s): got %s", c.s, s)
		}
	}
}

func TestKeyOrdering(t *testing.T) {
	keys := [][]byte{
		encoding.MakeBareKey(1, 1, nil),
		encoding.MakeProposalKey(1, 1, nil),
		encoding.MakeProposedWriteKey(1, 1, nil, 123456),
		encoding.MakeProposedWriteKey(1, 1, nil, 123),
		encoding.MakeVersionKey(1, 1, nil, 999999),
		encoding.MakeVersionKey(1, 1, nil, 99999),
		encoding.MakeNextKey(encoding.MakeVersionKey(1, 1, nil, 99999)),
		encoding.MakeVersionKey(1, 1, nil, 99),
		encoding.MakeTransactionKey(1, 1, nil, 123456789),
		encoding.MakeNextBareKey(encoding.MakeBareKey(1, 1, nil)),

		encoding.MakeBareKey(1, 2, []sql.Value{nil}),
		encoding.MakeBareKey(1, 2, []sql.Value{sql.BoolValue(false)}),
		encoding.MakeNextBareKey(encoding.MakeBareKey(1, 2, []sql.Value{sql.BoolValue(false)})),
		encoding.MakeBareKey(1, 2, []sql.Value{sql.BoolValue(true)}),
		encoding.MakeBareKey(1, 2, []sql.Value{sql.StringValue("")}),
		encoding.MakeBareKey(1, 2, []sql.Value{sql.StringValue(""), nil}),
		encoding.MakeBareKey(1, 2, []sql.Value{sql.StringValue(""), sql.StringValue("")}),

		encoding.MakeBareKey(1, 3, []sql.Value{sql.Int64Value(-3456)}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.Int64Value(-345)}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.Int64Value(0)}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.Int64Value(34)}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.Int64Value(345)}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.Float64Value(math.NaN())}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.Float64Value(-123456.789)}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.Float64Value(-0.0012345)}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.Float64Value(0.0)}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.Float64Value(12.345)}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.Float64Value(1234567890000)}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.StringValue("")}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.StringValue("abc")}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.StringValue("abcd")}),
		encoding.MakeBareKey(1, 3, []sql.Value{sql.StringValue("bc")}),

		encoding.MakeBareKey(2, 0, nil),
		encoding.MakeBareKey(2, 1, nil),
		encoding.MakeBareKey(2, 2, nil),
		encoding.MakeBareKey(2, 2, []sql.Value{sql.Int64Value(3)}),
		encoding.MakeBareKey(2, 3, nil),

		encoding.MakeBareKey(3, 0, nil),
		encoding.MakeBareKey(3, 0, []sql.Value{sql.Int64Value(123), sql.StringValue("abc")}),
		encoding.MakeBareKey(3, 0, []sql.Value{sql.Int64Value(123), sql.StringValue("def")}),
		encoding.MakeBareKey(3, 0, []sql.Value{sql.Int64Value(456), sql.StringValue("abc")}),
	}

	for idx := 1; idx < len(keys); idx += 1 {
		if bytes.Compare(keys[idx-1], keys[idx]) != -1 {
			t.Errorf("%s not less than %s", encoding.FormatKey(keys[idx-1]),
				encoding.FormatKey(keys[idx]))
		}
	}
}
