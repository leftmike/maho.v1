package encode

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/util"
)

const (
	boolValueTag    = 1
	int64ValueTag   = 2
	float64ValueTag = 3
	stringValueTag  = 4
	bytesValueTag   = 5
	// Value tags must be less than 16.
)

func encodeColNumValueTag(buf []byte, colNum int, tag byte) []byte {
	if colNum == 0 {
		buf = append(buf, tag)
	} else if colNum < 16 {
		buf = append(buf, byte(colNum<<4)|tag)
	} else {
		buf = append(buf, 0xF0|tag)
		buf = util.EncodeVarint(buf, uint64(colNum))
	}
	return buf
}

func EncodeRowValue(row []sql.Value) []byte {
	if len(row) == 0 {
		panic("encode row value called with zero length row")
	}

	buf := util.EncodeVarint(nil, uint64(len(row)))
	for num := range row {
		val := row[num]
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
			buf = util.EncodeVarint(buf, uint64(len(b)))
			buf = append(buf, b...)
		case sql.BytesValue:
			b := []byte(val)
			buf = encodeColNumValueTag(buf, num, bytesValueTag)
			buf = util.EncodeVarint(buf, uint64(len(b)))
			buf = append(buf, b...)
		case sql.Float64Value:
			buf = encodeColNumValueTag(buf, num, float64ValueTag)
			buf = util.EncodeUint64(buf, math.Float64bits(float64(val)))
		case sql.Int64Value:
			buf = encodeColNumValueTag(buf, num, int64ValueTag)
			buf = util.EncodeZigzag64(buf, int64(val))
		default:
			panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", val, val))
		}
	}
	return buf
}

func DecodeRowValue(buf []byte) []sql.Value {
	var ok bool
	var u uint64

	buf, u, ok = util.DecodeVarint(buf)
	if !ok {
		return nil
	}
	dest := make([]sql.Value, u)

	for len(buf) > 0 {
		tag := buf[0] & 0x0F
		num := int(buf[0] >> 4)
		buf = buf[1:]
		if num == 16 {
			buf, u, ok = util.DecodeVarint(buf)
			if !ok {
				return nil
			}
			num = int(u)
		}

		var val sql.Value
		switch tag {
		case boolValueTag:
			if len(buf) < 1 {
				return nil
			}
			if buf[0] == 0 {
				val = sql.BoolValue(false)
			} else {
				val = sql.BoolValue(true)
			}
			buf = buf[1:]
		case stringValueTag:
			buf, u, ok = util.DecodeVarint(buf)
			if !ok {
				return nil
			}
			if len(buf) < int(u) {
				return nil
			}
			val = sql.StringValue(buf[:u])
			buf = buf[u:]
		case bytesValueTag:
			buf, u, ok = util.DecodeVarint(buf)
			if !ok {
				return nil
			}
			if len(buf) < int(u) {
				return nil
			}
			val = sql.BytesValue(buf[:u])
			buf = buf[u:]
		case float64ValueTag:
			if len(buf) < 8 {
				return nil
			}
			u = binary.BigEndian.Uint64(buf)
			val = sql.Float64Value(math.Float64frombits(u))
			buf = buf[8:]
		case int64ValueTag:
			var n int64
			buf, n, ok = util.DecodeZigzag64(buf)
			if !ok {
				return nil
			}
			val = sql.Int64Value(n)
		default:
			return nil
		}

		if num >= len(dest) {
			return nil
		}
		dest[num] = val
	}

	return dest
}
