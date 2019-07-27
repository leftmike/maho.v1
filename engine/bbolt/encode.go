package bbolt

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

const (
	nullKeyTag        = 0
	boolKeyTag        = 1
	int64NegKeyTag    = 2
	int64NotNegKeyTag = 3
	float64NaNKeyTag  = 4
	float64NegKeyTag  = 5
	float64ZeroKeyTag = 6
	float64PosKeyTag  = 7
	stringKeyTag      = 8
)

func encodeUInt64(buf []byte, u uint64) []byte {
	// Use binary.BigEndian.Uint64 to decode.
	return append(buf, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
		byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func encodeKeyBytes(buf []byte, bytes []byte) []byte {
	for _, b := range bytes {
		if b == 0 || b == 1 {
			buf = append(buf, 1)
		}
		buf = append(buf, b)
	}
	return append(buf, 0)
}

func decodeKeyBytes(key []byte) ([]byte, []byte, bool) {
	var bytes []byte
	var esc bool
	for idx, b := range key {
		if esc {
			bytes = append(bytes, b)
			esc = false
		} else if b == 0 {
			return key[idx+1:], bytes, true
		} else if b == 1 {
			esc = true
		} else {
			bytes = append(bytes, b)
		}
	}
	return nil, nil, false
}

func MakeKey(row []sql.Value, colKeys []engine.ColumnKey) []byte {
	var key []byte

	for _, ck := range colKeys {
		num := ck.Number()
		if num >= len(row) {
			key = append(key, nullKeyTag)
		} else {
			switch val := row[num].(type) {
			case sql.BoolValue:

				key = append(key, boolKeyTag)
				if val {
					key = append(key, 1)
				} else {
					key = append(key, 0)
				}
			case sql.StringValue:
				key = append(key, stringKeyTag)
				key = encodeKeyBytes(key, []byte(val))
			case sql.Float64Value:
				if math.IsNaN(float64(val)) {
					key = append(key, float64NaNKeyTag)
				} else if val == 0 {
					key = append(key, float64ZeroKeyTag)
				} else {
					u := math.Float64bits(float64(val))
					if u&(1<<63) != 0 {
						u = ^u
						key = append(key, float64NegKeyTag)
					} else {
						key = append(key, float64PosKeyTag)
					}
					key = encodeUInt64(key, u)
				}
			case sql.Int64Value:
				if val < 0 {
					key = append(key, int64NegKeyTag)
				} else {
					key = append(key, int64NotNegKeyTag)
				}
				key = encodeUInt64(key, uint64(val))
			default:
				if val == nil {
					key = append(key, nullKeyTag)
				} else {
					panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", val, val))
				}
			}
		}
	}

	return key
}

func ParseKey(key []byte, colKeys []engine.ColumnKey, dest []sql.Value) bool {
	for _, ck := range colKeys {
		if len(key) < 1 {
			return false
		}

		var val sql.Value
		switch key[0] {
		case nullKeyTag:
			val = nil
			key = key[1:]
		case boolKeyTag:
			if len(key) < 1 {
				return false
			}
			if key[1] == 0 {
				val = sql.BoolValue(false)
			} else {
				val = sql.BoolValue(true)
			}
			key = key[2:]
		case stringKeyTag:
			var s []byte
			var ok bool
			key, s, ok = decodeKeyBytes(key[1:])
			if !ok {
				return false
			}
			val = sql.StringValue(s)
		case float64NaNKeyTag:
			val = sql.Float64Value(math.NaN())
			key = key[1:]
		case float64NegKeyTag:
			var u uint64
			if len(key) < 9 {
				return false
			}
			u = ^binary.BigEndian.Uint64(key[1:])
			val = sql.Float64Value(math.Float64frombits(u))
			key = key[9:]
		case float64ZeroKeyTag:
			val = sql.Float64Value(0.0)
			key = key[1:]
		case float64PosKeyTag:
			var u uint64
			if len(key) < 9 {
				return false
			}
			u = binary.BigEndian.Uint64(key[1:])
			val = sql.Float64Value(math.Float64frombits(u))
			key = key[9:]
		case int64NegKeyTag, int64NotNegKeyTag:
			var u uint64
			if len(key) < 9 {
				return false
			}
			u = binary.BigEndian.Uint64(key[1:])
			val = sql.Int64Value(u)
			key = key[9:]
		default:
			return false
		}

		num := ck.Number()
		if num < len(dest) {
			dest[num] = val
		}
	}

	return true
}

func MakeValue(row []sql.Value) []byte {

	return nil
}

func ParseValue(buf []byte, dest []sql.Value) bool {

	return false
}
