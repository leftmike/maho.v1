package encoding_test

import (
	"math"
	"testing"

	proto "github.com/golang/protobuf/proto"
	"github.com/leftmike/maho/engine/kvrows/encoding"
	"github.com/leftmike/maho/testutil"
)

func TestEncodeVarint(t *testing.T) {
	numbers := []uint64{
		0,
		1,
		125,
		126,
		127,
		0xFF,
		0x100,
		0xFFF,
		0x1000,
		0x7F7F,
		1234567890,
		math.MaxUint32,
		math.MaxUint64,
	}

	for _, n := range numbers {
		buf := encoding.EncodeVarint(nil, n)
		pbuf := proto.EncodeVarint(n)
		if !testutil.DeepEqual(buf, pbuf) {
			t.Errorf("EncodeVarint(%d): got %v want %v", n, buf, pbuf)
		}
		rbuf, r, ok := encoding.DecodeVarint(buf)
		if !ok {
			t.Errorf("DecodeVarint(%v) failed", buf)
		} else if len(rbuf) != 0 {
			t.Errorf("DecodeVarint(%v): got %v want []", buf, rbuf)
		} else if n != r {
			t.Errorf("DecodeVarint(%v): got %d want %d", buf, r, n)
		}
	}
}

func TestEncodeZigzag64(t *testing.T) {
	numbers := []int64{
		0,
		1,
		125,
		126,
		127,
		128,
		129,
		0xFF,
		0x100,
		0xFFF,
		0x1000,
		0x7F7F,
		1234567890,
		10000000000,
		math.MaxInt32,
		math.MaxInt64,
		math.MinInt32,
		math.MinInt64,
		-987654321,
		-1000000000,
		-125,
		-126,
		-127,
		-128,
		-129,
		-0xFF,
	}

	for _, n := range numbers {
		buf := encoding.EncodeZigzag64(nil, n)
		//pbuf := proto.EncodeVarint(n)
		//if !testutil.DeepEqual(buf, pbuf) {
		//	t.Errorf("EncodeVarint(%d): got %v want %v", n, buf, pbuf)
		//}
		rbuf, r, ok := encoding.DecodeZigzag64(buf)
		if !ok {
			t.Errorf("DecodeZigzag64(%v) failed", buf)
		} else if len(rbuf) != 0 {
			t.Errorf("DecodeZigzag64(%v): got %v want []", buf, rbuf)
		} else if n != r {
			t.Errorf("DecodeZigzag64(%v): got %d want %d", buf, r, n)
		}
	}
}

func testPanic(t *testing.T, n string, test func()) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("%s: did not fail", n)
		}
	}()

	test()
}

func TestMakeProtobufValue(t *testing.T) {
	testPanic(t, "MakeProtobufValue",
		func() {
			md := encoding.DatabaseMetadata{
				Type: uint32(127),
			}
			encoding.MakeProtobufValue(&md)
		})

	testPanic(t, "MakeProtobufValue",
		func() {
			md := encoding.DatabaseMetadata{}
			encoding.MakeProtobufValue(&md)
		})
	md := encoding.DatabaseMetadata{
		Type: uint32(encoding.Type_DatabaseMetadataType),
	}
	encoding.MakeProtobufValue(&md)

	testPanic(t, "MakeProtobufValue",
		func() {
			td := encoding.TableMetadata{}
			encoding.MakeProtobufValue(&td)
		})
	td := encoding.TableMetadata{
		Type: uint32(encoding.Type_TableMetadataType),
	}
	encoding.MakeProtobufValue(&td)
}
