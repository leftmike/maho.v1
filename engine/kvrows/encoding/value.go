package encoding

import (
	"encoding/binary"
	"fmt"
	"math"

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
	// Tags must be less than 16.
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
	// Copied from github.com/golang/protobuf/proto/decode.go
	buf, n, ok := DecodeVarint(buf)
	if !ok {
		return nil, 0, false
	}
	return buf, int64((n >> 1) ^ uint64((int64(n&1)<<63)>>63)), true
}

func encodeCdxValueTag(buf []byte, cdx int, tag byte) []byte {
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
			b := []byte(val)
			buf = encodeCdxValueTag(buf, cdx, stringValueTag)
			buf = EncodeVarint(buf, uint64(len(b)))
			buf = append(buf, b...)
		case sql.Float64Value:
			buf = encodeCdxValueTag(buf, cdx, float64ValueTag)
			buf = encodeUInt64(buf, math.Float64bits(float64(val)))
		case sql.Int64Value:
			buf = encodeCdxValueTag(buf, cdx, int64ValueTag)
			buf = EncodeZigzag64(buf, int64(val))
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
	// The Type field is required, so there always must be at least 2 bytes.
	return len(buf) > 1 && buf[0] == valueIsProtobuf
}

func setRowResult(ret []sql.Value, cdx int, val sql.Value) []sql.Value {
	for len(ret) <= cdx {
		ret = append(ret, nil)
	}
	ret[cdx] = val
	return ret
}

func ParseRowValue(buf []byte) ([]sql.Value, bool) {
	if len(buf) < 1 || buf[0] != valueIsRow {
		return nil, false
	}
	buf = buf[1:]

	var ok bool
	var u uint64
	var ret []sql.Value

	for len(buf) > 0 {
		tag := buf[0] & 0x0F
		cdx := int(buf[0] >> 4)
		buf = buf[1:]
		if cdx == 16 {
			buf, u, ok = DecodeVarint(buf)
			if !ok {
				return nil, false
			}
			cdx = int(u)
		}
		switch tag {
		case boolValueTag:
			if len(buf) < 1 {
				return nil, false
			}
			if buf[0] == 0 {
				ret = setRowResult(ret, cdx, sql.BoolValue(false))
			} else {
				ret = setRowResult(ret, cdx, sql.BoolValue(true))
			}
			buf = buf[1:]
		case stringValueTag:
			buf, u, ok = DecodeVarint(buf)
			if !ok {
				return nil, false
			}
			if len(buf) < int(u) {
				return nil, false
			}
			ret = setRowResult(ret, cdx, sql.StringValue(buf[:u]))
			buf = buf[u:]
		case float64ValueTag:
			if len(buf) < 8 {
				return nil, false
			}
			u = binary.BigEndian.Uint64(buf)
			ret = setRowResult(ret, cdx, sql.Float64Value(math.Float64frombits(u)))
			buf = buf[8:]
		case int64ValueTag:
			var n int64
			buf, n, ok = DecodeZigzag64(buf)
			if !ok {
				return nil, false
			}
			ret = setRowResult(ret, cdx, sql.Int64Value(n))
		default:
			return nil, false
		}
	}

	return ret, true
}

func ParseProtobufValue(buf []byte, pb proto.Message) bool {
	if !IsProtobufValue(buf) {
		return false
	}
	return proto.Unmarshal(buf, pb) == nil
}

func formatBadValue(msg string, buf []byte) string {
	if len(buf) > 30 {
		return fmt.Sprintf("%s: %v...", msg, buf[:30])
	}
	return fmt.Sprintf("%s: %v", msg, buf)
}

func formatProtobufValue(buf []byte) string {
	switch Type(buf[1]) {
	case Type_DatabaseMetadataType:
		md := DatabaseMetadata{}
		if ParseProtobufValue(buf, &md) {
			return fmt.Sprintf("%v", md)
		}
	case Type_TableMetadataType:
		td := TableMetadata{}
		if ParseProtobufValue(buf, &td) {
			return fmt.Sprintf("%v", td)
		}
	case Type_TransactionType:
		tx := Transaction{}
		if ParseProtobufValue(buf, &tx) {
			return fmt.Sprintf("%v", tx)
		}
	case Type_ProposalType:
		pr := Proposal{}
		if ParseProtobufValue(buf, &pr) {
			return fmt.Sprintf("%v", pr)
		}
	}
	return formatBadValue("bad protobuf value", buf)
}

func FormatValue(buf []byte) string {
	if IsRowValue(buf) {
		vals, ok := ParseRowValue(buf)
		if !ok {
			return formatBadValue("bad row value", buf)
		}

		var s string
		for cdx, val := range vals {
			if cdx > 0 {
				s += ", "
			}
			s += sql.Format(val)
		}
		return s
	} else if IsProtobufValue(buf) {
		return formatProtobufValue(buf)
	}

	return formatBadValue("bad value", buf)
}

func init() {
	if Type_MaximumType != Type_ProposalType {
		panic("new Type added to metadata.proto without updating formatProtobufValue")
	}
}
