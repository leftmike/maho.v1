package encode_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage/encode"
)

func testMakeKey(t *testing.T, key []sql.ColumnKey, values []sql.Value,
	makeRow func(val sql.Value) []sql.Value) {

	var prev []byte
	for _, val := range values {
		row := makeRow(val)
		buf := encode.MakeKey(key, row)
		if bytes.Compare(prev, buf) >= 0 {
			t.Errorf("MakeKey() not greater: %s", row)
		}
		prev = buf
	}
}

func TestMakeKey(t *testing.T) {
	values := []sql.Value{
		nil,
		sql.BoolValue(false),
		sql.BoolValue(true),
		sql.Int64Value(-999),
		sql.Int64Value(-9),
		sql.Int64Value(0),
		sql.Int64Value(9),
		sql.Int64Value(999),
		sql.Float64Value(math.NaN()),
		sql.Float64Value(-999.9),
		sql.Float64Value(-9.9),
		sql.Float64Value(0.0),
		sql.Float64Value(9.9),
		sql.Float64Value(999.9),
		sql.StringValue("A"),
		sql.StringValue("AA"),
		sql.StringValue("AAA"),
		sql.StringValue("AB"),
		sql.StringValue("BBB"),
		sql.StringValue("aaa"),
		sql.BytesValue([]byte{0}),
		sql.BytesValue([]byte{0, 0}),
		sql.BytesValue([]byte{0, 0, 0}),
		sql.BytesValue([]byte{0, 1}),
		sql.BytesValue([]byte{1, 1}),
		sql.BytesValue([]byte{2, 0, 0, 0, 1}),
		sql.BytesValue([]byte{2, 0, 0, 1}),
		sql.BytesValue([]byte{2, 0, 0, 2}),
		sql.BytesValue([]byte{2, 2, 0, 0}),
		sql.BytesValue([]byte{254, 0}),
		sql.BytesValue([]byte{254, 0, 0}),
		sql.BytesValue([]byte{254, 255}),
		sql.BytesValue([]byte{255}),
	}

	reverseValues := []sql.Value{
		nil,
		sql.BoolValue(true),
		sql.BoolValue(false),
		sql.Int64Value(999),
		sql.Int64Value(9),
		sql.Int64Value(0),
		sql.Int64Value(-9),
		sql.Int64Value(-999),
		sql.Float64Value(999.9),
		sql.Float64Value(9.9),
		sql.Float64Value(0.0),
		sql.Float64Value(-9.9),
		sql.Float64Value(-999.9),
		sql.Float64Value(math.NaN()),
		sql.StringValue("aaa"),
		sql.StringValue("BBB"),
		sql.StringValue("AB"),
		sql.StringValue("AAA"),
		sql.StringValue("AA"),
		sql.StringValue("A"),
		sql.BytesValue([]byte{255}),
		sql.BytesValue([]byte{254, 255}),
		sql.BytesValue([]byte{254, 0, 0}),
		sql.BytesValue([]byte{254, 0}),
		sql.BytesValue([]byte{2, 2, 0, 0}),
		sql.BytesValue([]byte{2, 0, 0, 2}),
		sql.BytesValue([]byte{2, 0, 0, 1}),
		sql.BytesValue([]byte{2, 0, 0, 0, 1}),
		sql.BytesValue([]byte{1, 1}),
		sql.BytesValue([]byte{0, 1}),
		sql.BytesValue([]byte{0, 0, 0}),
		sql.BytesValue([]byte{0, 0}),
		sql.BytesValue([]byte{0}),
	}

	testMakeKey(t, []sql.ColumnKey{sql.MakeColumnKey(0, false)}, values,
		func(val sql.Value) []sql.Value {
			return []sql.Value{val}
		})

	for _, val0 := range values {
		testMakeKey(t, []sql.ColumnKey{
			sql.MakeColumnKey(1, false),
			sql.MakeColumnKey(0, false),
		}, values,
			func(val1 sql.Value) []sql.Value {
				return []sql.Value{val0, val1}
			})
	}

	testMakeKey(t, []sql.ColumnKey{sql.MakeColumnKey(0, true)}, reverseValues,
		func(val sql.Value) []sql.Value {
			return []sql.Value{val}
		})

	for _, val0 := range reverseValues {
		testMakeKey(t, []sql.ColumnKey{
			sql.MakeColumnKey(1, false),
			sql.MakeColumnKey(0, true),
		}, values,
			func(val1 sql.Value) []sql.Value {
				return []sql.Value{val0, val1}
			})
	}

	for _, val0 := range values {
		testMakeKey(t, []sql.ColumnKey{
			sql.MakeColumnKey(0, false),
			sql.MakeColumnKey(1, true),
		}, reverseValues,
			func(val1 sql.Value) []sql.Value {
				return []sql.Value{val0, val1}
			})
	}
}
