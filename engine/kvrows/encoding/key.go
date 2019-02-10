package encoding

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/leftmike/maho/sql"
)

const (
	NullKeyTag        = 128
	BoolKeyTag        = 129
	Int64NegKeyTag    = 130
	Int64NotNegKeyTag = 131
	Float64NaNKeyTag  = 140
	Float64NegKeyTag  = 141
	Float64ZeroKeyTag = 142
	Float64PosKeyTag  = 143
	StringKeyTag      = 150
)

func MakeKey(tid, iid uint32, vals ...sql.Value) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint32(key, tid)
	binary.BigEndian.PutUint32(key[4:], iid)

	for _, val := range vals {
		switch val := val.(type) {
		case sql.BoolValue:
			key = append(key, BoolKeyTag)
			if val {
				key = append(key, 1)
			} else {
				key = append(key, 0)
			}
		case sql.StringValue:
			key = append(key, StringKeyTag)
			key = encodeKeyBytes(key, []byte(val))
		case sql.Float64Value:
			if math.IsNaN(float64(val)) {
				key = append(key, Float64NaNKeyTag)
			} else if val == 0 {
				key = append(key, Float64ZeroKeyTag)
			} else {
				u := math.Float64bits(float64(val))
				if u&(1<<63) != 0 {
					u = ^u
					key = append(key, Float64NegKeyTag)
				} else {
					key = append(key, Float64PosKeyTag)
				}
				key = encodeUInt64(key, u)
			}
		case sql.Int64Value:
			if val < 0 {
				key = append(key, Int64NegKeyTag)
			} else {
				key = append(key, Int64NotNegKeyTag)
			}
			key = encodeUInt64(key, uint64(val))
		default:
			if val == nil {
				key = append(key, NullKeyTag)
			} else {
				panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", val, val))
			}
		}
	}
	return key
}

func encodeUInt64(key []byte, u uint64) []byte {
	return append(key, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
		byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func decodeUInt64(key []byte) ([]byte, uint64, bool) {
	if len(key) < 8 {
		return nil, 0, false
	}
	return key[8:], uint64(key[7]) | uint64(key[6])<<8 | uint64(key[5])<<16 | uint64(key[4])<<24 |
		uint64(key[3])<<32 | uint64(key[2])<<40 | uint64(key[1])<<48 | uint64(key[0])<<56, true
}

func encodeKeyBytes(key []byte, bytes []byte) []byte {
	for _, b := range bytes {
		if b == 0 || b == 1 {
			key = append(key, 1)
		}
		key = append(key, b)
	}
	return append(key, 0)
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

func ParseKey(key []byte, tid, iid uint32) ([]sql.Value, bool) {
	if len(key) < 8 {
		return nil, false
	}
	if binary.BigEndian.Uint32(key) != tid || binary.BigEndian.Uint32(key[4:]) != iid {
		return nil, false
	}
	key = key[8:]

	var vals []sql.Value
	for len(key) > 0 {
		switch key[0] {
		case NullKeyTag:
			vals = append(vals, nil)
			key = key[1:]
		case BoolKeyTag:
			if len(key) < 1 {
				return nil, false
			}
			if key[1] == 0 {
				vals = append(vals, sql.BoolValue(false))
			} else {
				vals = append(vals, sql.BoolValue(true))
			}
			key = key[2:]
		case StringKeyTag:
			var s []byte
			var ok bool
			key, s, ok = decodeKeyBytes(key[1:])
			if !ok {
				return nil, false
			}
			vals = append(vals, sql.StringValue(s))
		case Float64NaNKeyTag:
			vals = append(vals, sql.Float64Value(math.NaN()))
			key = key[1:]
		case Float64NegKeyTag:
			var u uint64
			var ok bool
			key, u, ok = decodeUInt64(key[1:])
			if !ok {
				return nil, false
			}
			u = ^u
			vals = append(vals, sql.Float64Value(math.Float64frombits(u)))
		case Float64ZeroKeyTag:
			vals = append(vals, sql.Float64Value(0.0))
			key = key[1:]
		case Float64PosKeyTag:
			var u uint64
			var ok bool
			key, u, ok = decodeUInt64(key[1:])
			if !ok {
				return nil, false
			}
			vals = append(vals, sql.Float64Value(math.Float64frombits(u)))
		case Int64NegKeyTag, Int64NotNegKeyTag:
			var u uint64
			var ok bool
			key, u, ok = decodeUInt64(key[1:])
			if !ok {
				return nil, false
			}
			vals = append(vals, sql.Int64Value(u))
		default:
			return nil, false
		}
	}

	return vals, true
}

func FormatKey(key []byte) string {
	if len(key) < 8 {
		return fmt.Sprintf("bad key: %v", key)
	}

	tid := binary.BigEndian.Uint32(key)
	iid := binary.BigEndian.Uint32(key[4:])
	vals, ok := ParseKey(key, tid, iid)
	if !ok {
		return fmt.Sprintf("bad key: %v", key)
	}

	s := fmt.Sprintf("/%d/%d", tid, iid)
	for _, val := range vals {
		if sv, ok := val.(sql.StringValue); ok {
			s = fmt.Sprintf("%s/%s", s, string(sv))
		} else {
			s = fmt.Sprintf("%s/%s", s, sql.Format(val))
		}
	}
	return s
}
