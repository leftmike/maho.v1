package kvrows

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

const (
	BareKeyType     = 0
	ProposalKeyType = 1
	DurableKeyType  = 2
	UnknownKeyType  = 255

	nullKeyTag        = 130
	boolKeyTag        = 131
	int64NegKeyTag    = 140
	int64NotNegKeyTag = 141
	float64NaNKeyTag  = 150
	float64NegKeyTag  = 151
	float64ZeroKeyTag = 152
	float64PosKeyTag  = 153
	stringKeyTag      = 160

	tombstoneValue = 0
	rowValue       = 1

	boolValueTag    = 1
	int64ValueTag   = 2
	float64ValueTag = 3
	stringValueTag  = 4
	// Value tags must be less than 16.
)

func EncodeUint64(u uint64) []byte {
	return encodeUint64(make([]byte, 0, 8), false, u)
}

func encodeUint64(buf []byte, reverse bool, u uint64) []byte {
	if reverse {
		u = ^u
	}
	return append(buf, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
		byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func DecodeUint64(buf []byte) uint64 {
	return decodeUint64(buf, false)
}

func decodeUint64(buf []byte, reverse bool) uint64 {
	u := binary.BigEndian.Uint64(buf)
	if reverse {
		u = ^u
	}
	return u
}

func encodeUint32(buf []byte, reverse bool, u uint32) []byte {
	if reverse {
		u = ^u
	}
	return append(buf, byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func decodeUint32(buf []byte, reverse bool) uint32 {
	u := binary.BigEndian.Uint32(buf)
	if reverse {
		u = ^u
	}
	return u
}

func encodeKeyBytes(buf []byte, reverse bool, bytes []byte) []byte {
	for _, b := range bytes {
		if b == 0 || b == 1 {
			buf = encodeByte(buf, reverse, 1)
		}
		buf = encodeByte(buf, reverse, b)
	}
	return encodeByte(buf, reverse, 0)
}

func decodeKeyBytes(key []byte, reverse bool) ([]byte, []byte, bool) {
	var bytes []byte
	var esc bool
	for idx, b := range key {
		if reverse {
			b = ^b
		}
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

func encodeByte(buf []byte, reverse bool, b byte) []byte {
	if reverse {
		return append(buf, ^b)
	}
	return append(buf, b)
}

func decodeByte(reverse bool, b byte) byte {
	if reverse {
		return ^b
	}
	return b
}

func MakePrefix(row []sql.Value, colKeys []engine.ColumnKey) []byte {
	if len(colKeys) < 1 || len(colKeys) > 255 {
		panic(fmt.Sprintf("a column key must have been 1 and 255 columns: %d", len(colKeys)))
	}

	key := []byte{byte(len(colKeys))}
	for _, ck := range colKeys {
		num := ck.Number()
		if num >= len(row) {
			key = encodeByte(key, ck.Reverse(), nullKeyTag)
		} else {
			switch val := row[num].(type) {
			case sql.BoolValue:
				key = encodeByte(key, ck.Reverse(), boolKeyTag)
				if val {
					key = encodeByte(key, ck.Reverse(), 1)
				} else {
					key = encodeByte(key, ck.Reverse(), 0)
				}
			case sql.StringValue:
				key = encodeByte(key, ck.Reverse(), stringKeyTag)
				key = encodeKeyBytes(key, ck.Reverse(), []byte(val))
			case sql.Float64Value:
				if math.IsNaN(float64(val)) {
					key = encodeByte(key, ck.Reverse(), float64NaNKeyTag)
				} else if val == 0 {
					key = encodeByte(key, ck.Reverse(), float64ZeroKeyTag)
				} else {
					u := math.Float64bits(float64(val))
					if u&(1<<63) != 0 {
						u = ^u
						key = encodeByte(key, ck.Reverse(), float64NegKeyTag)
					} else {
						key = encodeByte(key, ck.Reverse(), float64PosKeyTag)
					}
					key = encodeUint64(key, ck.Reverse(), u)
				}
			case sql.Int64Value:
				if val < 0 {
					key = encodeByte(key, ck.Reverse(), int64NegKeyTag)
				} else {
					key = encodeByte(key, ck.Reverse(), int64NotNegKeyTag)
				}
				key = encodeUint64(key, ck.Reverse(), uint64(val))
			default:
				if val == nil {
					key = encodeByte(key, ck.Reverse(), nullKeyTag)
				} else {
					panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", val, val))
				}
			}
		}
	}

	return key
}

func MakeBareKey(row []sql.Value, colKeys []engine.ColumnKey) []byte {
	// <sql-key> 0x00
	key := MakePrefix(row, colKeys)
	key = append(key, BareKeyType)
	return key
}

func MakeProposalKey(row []sql.Value, colKeys []engine.ColumnKey, tid, sid uint32) []byte {
	// <sql-key> 0x01 <tid> <sid> 0x01
	key := MakePrefix(row, colKeys)
	key = append(key, ProposalKeyType)
	key = encodeUint32(key, false, tid)
	// Encode the statement id _descending_ so that the most recent statement id will be
	// encountered first in a key scan.
	key = encodeUint32(key, true, sid)
	key = append(key, ProposalKeyType)
	return key
}

func MakeDurableKey(row []sql.Value, colKeys []engine.ColumnKey, version uint64) []byte {
	// <sql-key> 0x02 <version> 0x02
	key := MakePrefix(row, colKeys)
	key = append(key, DurableKeyType)
	// Encode the version _descending_ so that the most recent version will be encountered
	// first in a key scan.
	key = encodeUint64(key, true, version)
	key = append(key, DurableKeyType)
	return key
}

func GetKeyType(key []byte) byte {
	if len(key) == 0 {
		return UnknownKeyType
	}
	return key[0]
}

func parseKey(key []byte, colKeys []engine.ColumnKey, dest []sql.Value) bool {
	if len(key) < 1 || key[0] != byte(len(colKeys)) {
		return false
	}
	key = key[1:]

	for _, ck := range colKeys {
		if len(key) < 1 {
			return false
		}

		var val sql.Value
		switch decodeByte(ck.Reverse(), key[0]) {
		case nullKeyTag:
			val = nil
			key = key[1:]
		case boolKeyTag:
			if len(key) < 1 {
				return false
			}
			if decodeByte(ck.Reverse(), key[1]) == 0 {
				val = sql.BoolValue(false)
			} else {
				val = sql.BoolValue(true)
			}
			key = key[2:]
		case stringKeyTag:
			var s []byte
			var ok bool
			key, s, ok = decodeKeyBytes(key[1:], ck.Reverse())
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
			u = ^decodeUint64(key[1:], ck.Reverse())
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
			u = decodeUint64(key[1:], ck.Reverse())
			val = sql.Float64Value(math.Float64frombits(u))
			key = key[9:]
		case int64NegKeyTag, int64NotNegKeyTag:
			var u uint64
			if len(key) < 9 {
				return false
			}
			u = decodeUint64(key[1:], ck.Reverse())
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

func ParseBareKey(key []byte, colKeys []engine.ColumnKey, dest []sql.Value) bool {
	if len(key) == 0 || key[len(key)-1] != BareKeyType {
		return false
	}
	return parseKey(key[:len(key)-1], colKeys, dest)
}

func ParseProposalKey(key []byte, colKeys []engine.ColumnKey, dest []sql.Value) (tid, sid uint32,
	ok bool) {

	if len(key) < 10 || key[len(key)-1] != ProposalKeyType || key[len(key)-10] != ProposalKeyType {
		return 0, 0, false
	}
	tid = decodeUint32(key[len(key)-9:len(key)-5], false)
	sid = decodeUint32(key[len(key)-5:len(key)-1], true)
	return tid, sid, parseKey(key[:len(key)-10], colKeys, dest)
}

func ParseDurableKey(key []byte, colKeys []engine.ColumnKey, dest []sql.Value) (version uint64,
	ok bool) {

	if len(key) < 10 || key[len(key)-1] != DurableKeyType || key[len(key)-10] != DurableKeyType {
		return 0, false
	}
	version = decodeUint64(key[len(key)-9:len(key)-1], true)
	return version, parseKey(key[:len(key)-10], colKeys, dest)
}

func EncodeVarint(buf []byte, n uint64) []byte {
	// Copied from github.com/golang/protobuf/proto/encode.go
	for n >= 1<<7 {
		buf = append(buf, uint8(n&0x7f|0x80))
		n >>= 7
	}
	return append(buf, uint8(n))
}

func DecodeVarint(buf []byte) ([]byte, uint64, bool) {
	// Copied from github.com/golang/protobuf/proto/decode.go
	var idx int
	var n uint64
	for shift := uint(0); shift < 64; shift += 7 {
		if idx >= len(buf) {
			return nil, 0, false
		}
		b := uint64(buf[idx])
		idx += 1
		n |= (b & 0x7F) << shift
		if (b & 0x80) == 0 {
			return buf[idx:], n, true
		}
	}

	// The number is too large to represent in a 64-bit value.
	return nil, 0, false
}

func EncodeZigzag64(buf []byte, n int64) []byte {
	// Copied from github.com/golang/protobuf/proto/encode.go
	return EncodeVarint(buf, uint64((uint64(n)<<1)^uint64(n>>63)))
}

func DecodeZigzag64(buf []byte) ([]byte, int64, bool) {
	// Copied from github.com/golang/protobuf/proto/decode.go
	buf, n, ok := DecodeVarint(buf)
	if !ok {
		return nil, 0, false
	}
	return buf, int64((n >> 1) ^ uint64((int64(n&1)<<63)>>63)), true
}

func encodeColNumValueTag(buf []byte, cdx int, tag byte) []byte {
	if cdx == 0 {
		buf = append(buf, tag)
	} else if cdx < 16 {
		buf = append(buf, byte(cdx<<4)|tag)
	} else {
		buf = append(buf, 0xF0|tag)
		buf = EncodeVarint(buf, uint64(cdx))
	}
	return buf
}

func MakeTombstoneValue() []byte {
	return []byte{tombstoneValue}
}

func MakeRowValue(row []sql.Value) []byte {
	buf := []byte{rowValue}
	for num, val := range row {
		if val == nil {
			continue
		}
		switch val := val.(type) {
		case sql.BoolValue:
			buf = encodeColNumValueTag(buf, num, boolValueTag)
			if val {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case sql.StringValue:
			b := []byte(val)
			buf = encodeColNumValueTag(buf, num, stringValueTag)
			buf = EncodeVarint(buf, uint64(len(b)))
			buf = append(buf, b...)
		case sql.Float64Value:
			buf = encodeColNumValueTag(buf, num, float64ValueTag)
			buf = encodeUint64(buf, false, math.Float64bits(float64(val)))
		case sql.Int64Value:
			buf = encodeColNumValueTag(buf, num, int64ValueTag)
			buf = EncodeZigzag64(buf, int64(val))
		default:
			panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", val, val))
		}
	}
	return buf
}

func IsTombstoneValue(buf []byte) bool {
	return len(buf) == 1 && buf[0] == tombstoneValue
}

func IsRowValue(buf []byte) bool {
	return len(buf) == 0 || buf[0] == rowValue
}

func ParseRowValue(buf []byte, dest []sql.Value) bool {
	if len(buf) > 0 {
		if buf[0] != rowValue {
			return false
		}
		buf = buf[1:]
	}

	var ok bool
	var u uint64

	for len(buf) > 0 {
		tag := buf[0] & 0x0F
		num := int(buf[0] >> 4)
		buf = buf[1:]
		if num == 16 {
			buf, u, ok = DecodeVarint(buf)
			if !ok {
				return false
			}
			num = int(u)
		}

		var val sql.Value
		switch tag {
		case boolValueTag:
			if len(buf) < 1 {
				return false
			}
			if buf[0] == 0 {
				val = sql.BoolValue(false)
			} else {
				val = sql.BoolValue(true)
			}
			buf = buf[1:]
		case stringValueTag:
			buf, u, ok = DecodeVarint(buf)
			if !ok {
				return false
			}
			if len(buf) < int(u) {
				return false
			}
			val = sql.StringValue(buf[:u])
			buf = buf[u:]
		case float64ValueTag:
			if len(buf) < 8 {
				return false
			}
			u = binary.BigEndian.Uint64(buf)
			val = sql.Float64Value(math.Float64frombits(u))
			buf = buf[8:]
		case int64ValueTag:
			var n int64
			buf, n, ok = DecodeZigzag64(buf)
			if !ok {
				return false
			}
			val = sql.Int64Value(n)
		default:
			return false
		}

		if num < len(dest) {
			dest[num] = val
		}
	}

	return true
}
