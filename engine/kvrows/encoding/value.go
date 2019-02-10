package encoding

import (
	"fmt"

	"github.com/golang/protobuf/proto"

	"github.com/leftmike/maho/sql"
)

const (
	valueIsRow      = 1
	valueIsProtobuf = 8 // If the first field of a protobuf is a varint, the first byte is 8.

	boolValueTag    = 1
	int64ValueTag   = 2
	float64ValueTag = 3
	stringValueTag  = 4
)

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
	buf, n, ok := DecodeVarint(buf)
	if !ok {
		return nil, 0, false
	}
	return buf, int64((n >> 1) ^ uint64((int64(n&1)<<63)>>63)), true
}

func encodeCdxValueTag(buf []byte, cdx int, tag byte) []byte {
	if cdx <= 0 {
		buf = append(buf, tag)
	} else if cdx < 16 {
		buf = append(buf, byte(cdx<<4)|tag)
	} else {
		buf = append(buf, 0xF0|tag)
		buf = EncodeVarint(buf, uint64(cdx))
	}
	return buf
}

func MakeRowValue(row []sql.Value) []byte {
	buf := []byte{valueIsRow}
	for cdx, val := range row {
		if val == nil {
			continue
		}
		switch val := val.(type) {
		case sql.BoolValue:
			buf = encodeCdxValueTag(buf, cdx, boolValueTag)
			if val {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case sql.StringValue:
			// XXX
		case sql.Float64Value:
			// XXX
		case sql.Int64Value:
			// XXX
		default:
			panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", val, val))
		}
	}
	return buf
}

func MakeProtobufValue(pb proto.Message) []byte {
	buf, err := proto.Marshal(pb)
	if err != nil {
		panic(fmt.Sprintf("error marshalling message: %s", err))
	}
	if len(buf) < 2 {
		panic("marshalled message must be at least two bytes long")
	}
	if buf[0] != valueIsProtobuf {
		panic(fmt.Sprintf("marshalled message must start with 0x%X: 0x%X", valueIsProtobuf, buf[0]))
	}
	if buf[1] == 0 || buf[1] > byte(Type_MaximumType) {
		panic(fmt.Sprintf("marshalled message: second byte must be > 0 and <= 0x%X: 0x%X",
			byte(Type_MaximumType), buf[1]))
	}
	return buf
}

func IsRowValue(buf []byte) bool {
	return len(buf) > 0 && buf[0] == valueIsRow
}

func IsProtobufValue(buf []byte) bool {
	return len(buf) > 0 && buf[0] == valueIsProtobuf
}

func ParseRowValue(buf []byte) ([]sql.Value, bool) {
	if !IsRowValue(buf) {
		return nil, false
	}
	// XXX
	return nil, false
}

func ParseProtobufValue(buf []byte, pb proto.Message) bool {
	if !IsProtobufValue(buf) {
		return false
	}
	return proto.Unmarshal(buf, pb) == nil
}

func FormatValue(buf []byte) string {
	// XXX
	return ""
}
