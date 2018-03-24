package mvcc

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/leftmike/maho/sql"
)

/*
A column number and associated value are encoded as follows:
    (buf[0] & 0xF0) == 0x70
    (buf[0] & 0x0F): value type
    buf[1]: column number
    type == 0: null value, no following bytes
    type == 1: false value, no following bytes
    type == 2: true value, no following bytes
    type == 3: double value, buf[2:10]: double
    type == 4: int64 value, buf[2:10]: int64
    type == 8: string16 value, buf[2:4]: length, buf[4:length+4]: bytes

Future:
    type == 5: int32 value, buf[2:6]: int32
    type == 6: int16 value, buf[2:4]: int16
    type == 7: int8 value, buf[2:3]: int8
    type == 9: string8 value, buf[2:3]: length, buf[3:length+3]: bytes

    (buf[0] & 0xF0) == 0xF0
    (buf[0] & 0x0F): value type
    buf[1:2]: column number

    (buf[0] & 0x80) == 0:
    ((buf[0] & 0x70) >> 4): value type (for a subset of value types)
    (buf[0] & 0x0F): column number

    big strings should be an array of page numbers for the contents
*/

var endian = binary.LittleEndian

const (
	nullValue     = 0
	falseValue    = 1
	trueValue     = 2
	doubleValue   = 3
	int64Value    = 4
	string16Value = 8
)

func decodeColVal(buf []byte) ([]byte, int, sql.Value, error) {
	if buf[0]&0xF0 != 0x70 {
		return nil, 0, nil,
			fmt.Errorf("column value must start with 0x70; got 0x%X", buf[0]&0xF0)
	}
	switch buf[0] & 0x0F {
	case nullValue:
		return buf[2:], int(buf[1]), nil, nil
	case falseValue:
		return buf[2:], int(buf[1]), sql.BoolValue(false), nil
	case trueValue:
		return buf[2:], int(buf[1]), sql.BoolValue(true), nil
	case doubleValue:
		return buf[10:], int(buf[1]),
			sql.Float64Value(math.Float64frombits(endian.Uint64(buf[2:]))), nil
	case int64Value:
		return buf[10:], int(buf[1]), sql.Int64Value(endian.Uint64(buf[2:])), nil
	case string16Value:
		bl := endian.Uint16(buf[2:])
		return buf[4+bl:], int(buf[1]), sql.StringValue(buf[4 : 4+bl]), nil
	}

	return nil, 0, nil, fmt.Errorf("unknown type for column value: %d", buf[0]&0x0F)
}

func encodedLength(col int, v sql.Value) (int, error) {
	if col < 0 || col > 255 {
		return 0, fmt.Errorf("column number must be >= 0 and < 256: %d", col)
	}
	el := 2
	if v != nil {
		switch v := v.(type) {
		case sql.BoolValue:
			// Nothing more.
		case sql.Int64Value:
			el += 8
		case sql.Float64Value:
			el += 8
		case sql.StringValue:
			bl := len(v)
			if bl > math.MaxUint16 {
				return 0, fmt.Errorf("string too long: %d", bl)
			}
			el += 2 + bl
		}
	}
	return el, nil
}

func encodeColVal(col int, v sql.Value, buf []byte) []byte {
	buf[1] = byte(col)
	if v == nil {
		buf[0] = 0x70 | nullValue
		return buf[2:]
	} else {
		switch v := v.(type) {
		case sql.BoolValue:
			if v {
				buf[0] = 0x70 | trueValue
			} else {
				buf[0] = 0x70 | falseValue
			}
			return buf[2:]
		case sql.Int64Value:
			buf[0] = 0x70 | int64Value
			endian.PutUint64(buf[2:], uint64(v))
			return buf[10:]
		case sql.Float64Value:
			buf[0] = 0x70 | doubleValue
			endian.PutUint64(buf[2:], math.Float64bits(float64(v)))
			return buf[10:]
		case sql.StringValue:
			bl := len(v)
			buf[0] = 0x70 | string16Value
			endian.PutUint16(buf[2:], uint16(bl))
			copy(buf[4:], v)
			return buf[4+bl:]
		}
	}
	return buf
}
