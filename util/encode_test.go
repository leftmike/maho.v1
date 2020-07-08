package util_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"

	"github.com/leftmike/maho/util"
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
		buf := util.EncodeVarint(nil, n)
		pbuf := proto.EncodeVarint(n)
		if !reflect.DeepEqual(buf, pbuf) {
			t.Errorf("EncodeVarint(%d): got %v want %v", n, buf, pbuf)
		}
		ret, r, ok := util.DecodeVarint(buf)
		if !ok {
			t.Errorf("DecodeVarint(%v) failed", buf)
		} else if len(ret) != 0 {
			t.Errorf("DecodeVarint(%v): got %v want []", buf, ret)
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
		buf := util.EncodeZigzag64(nil, n)
		enc := proto.NewBuffer(nil)
		err := enc.EncodeZigzag64(uint64(n))
		if err != nil {
			t.Errorf("proto.EncodeZigzag64(%d) failed with %s", n, err)
		} else {
			pbuf := enc.Bytes()
			if !reflect.DeepEqual(buf, pbuf) {
				t.Errorf("EncodeZigzag64(%d): got %v want %v", n, buf, pbuf)
			}
		}
		ret, r, ok := util.DecodeZigzag64(buf)
		if !ok {
			t.Errorf("DecodeZigzag64(%v) failed", buf)
		} else if len(ret) != 0 {
			t.Errorf("DecodeZigzag64(%v): got %v want []", buf, ret)
		} else if n != r {
			t.Errorf("DecodeZigzag64(%v): got %d want %d", buf, r, n)
		}
	}
}
