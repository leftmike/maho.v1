package encode

import (
	"fmt"
	"math"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/util"
)

const (
	// The SQL values are encoded as a tag followed by a binary representation
	// of the value.
	NullKeyTag              = 128
	BoolKeyTag              = 129
	Int64NegKeyTag          = 130
	Int64NotNegKeyTag       = 131
	Float64NaNKeyTag        = 140
	Float64NegKeyTag        = 141
	Float64ZeroKeyTag       = 142
	Float64PosKeyTag        = 143
	Float64NaNReverseKeyTag = 144
	StringKeyTag            = 150
	BytesKeyTag             = 160
)

func encodeKeyBytes(buf []byte, bytes []byte, reverse bool) []byte {
	n := len(buf)
	for _, b := range bytes {
		if b == 0 || b == 1 {
			buf = append(buf, 1)
		}
		buf = append(buf, b)
	}
	buf = append(buf, 0)

	if reverse {
		for n < len(buf) {
			buf[n] = ^buf[n]
			n += 1
		}
	}
	return buf
}

func MakeKey(key []sql.ColumnKey, row []sql.Value) []byte {
	var buf []byte

	for _, ck := range key {
		val := row[ck.Column()]
		reverse := ck.Reverse()

		switch val := val.(type) {
		case sql.BoolValue:
			if reverse {
				val = !val
			}
			buf = append(buf, BoolKeyTag)
			if val {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case sql.StringValue:
			buf = append(buf, StringKeyTag)
			buf = encodeKeyBytes(buf, []byte(val), reverse)
		case sql.BytesValue:
			buf = append(buf, BytesKeyTag)
			buf = encodeKeyBytes(buf, []byte(val), reverse)
		case sql.Float64Value:
			if reverse {
				val = -val
			}
			if math.IsNaN(float64(val)) {
				if reverse {
					buf = append(buf, Float64NaNReverseKeyTag)
				} else {
					buf = append(buf, Float64NaNKeyTag)
				}
			} else if val == 0 {
				buf = append(buf, Float64ZeroKeyTag)
			} else {
				u := math.Float64bits(float64(val))
				if u&(1<<63) != 0 {
					u = ^u
					buf = append(buf, Float64NegKeyTag)
				} else {
					buf = append(buf, Float64PosKeyTag)
				}
				buf = util.EncodeUint64(buf, u)
			}
		case sql.Int64Value:
			if reverse {
				val = ^val
			}
			if val < 0 {
				buf = append(buf, Int64NegKeyTag)
			} else {
				buf = append(buf, Int64NotNegKeyTag)
			}
			buf = util.EncodeUint64(buf, uint64(val))
		default:
			if val == nil {
				buf = append(buf, NullKeyTag)
			} else {
				panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", val, val))
			}
		}
	}
	return buf
}
