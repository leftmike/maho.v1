package little_test

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/leftmike/maho/engine/little"
)

var littleEndian bool

func init() {
	i := uint16(0x1234)
	littleEndian = ((*[2]byte)(unsafe.Pointer(&i))[0] == 0x34)
}

func TestTo(t *testing.T) {
	cases := []struct {
		i     int
		bytes []byte
		u16   uint16
		u32   uint32
		u64   uint64
	}{
		{0, []byte{0xFF, 0, 0, 0, 0, 0, 0, 0}, 0xFF, 0xFF, 0xFF},
		{3, []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD},
			0x5544, 0x77665544, 0xBBAA998877665544},
	}

	for _, c := range cases {
		var u16 uint16
		if littleEndian {
			u16 = *(*uint16)(unsafe.Pointer(&c.bytes[c.i]))
			if u16 != c.u16 {
				t.Fatalf("unsafe.Pointer(&%#v[%d]) got 0x%X want 0x%X", c.bytes, c.i, u16, c.u16)
			}
		}
		u16 = little.ToUint16(c.i, c.bytes)
		if u16 != c.u16 {
			t.Errorf("ToUint16(%d, %#v) got 0x%X want 0x%X", c.i, c.bytes, u16, c.u16)
		}

		var u32 uint32
		if littleEndian {
			u32 = *(*uint32)(unsafe.Pointer(&c.bytes[c.i]))
			if u32 != c.u32 {
				t.Fatalf("unsafe.Pointer(&%#v[%d]) got 0x%X want 0x%X", c.bytes, c.i, u32, c.u32)
			}
		}
		u32 = little.ToUint32(c.i, c.bytes)
		if u32 != c.u32 {
			t.Errorf("ToUint32(%d, %#v) got 0x%X want 0x%X", c.i, c.bytes, u32, c.u32)
		}

		var u64 uint64
		if littleEndian {
			u64 = *(*uint64)(unsafe.Pointer(&c.bytes[c.i]))
			if u64 != c.u64 {
				t.Fatalf("unsafe.Pointer(&%#v[%d]) got 0x%X want 0x%X", c.bytes, c.i, u64, c.u64)
			}
		}
		u64 = little.ToUint64(c.i, c.bytes)
		if u64 != c.u64 {
			t.Errorf("ToUint64(%d, %#v) got 0x%X want 0x%X", c.i, c.bytes, u64, c.u64)
		}
	}
}

func TestFrom(t *testing.T) {
	cases := []struct {
		i   int
		v   uint64
		b16 []byte
		b32 []byte
		b64 []byte
	}{
		{0, 0x0123456789ABCDEF, []byte{0xEF, 0xCD, 0}, []byte{0xEF, 0xCD, 0xAB, 0x89, 0},
			[]byte{0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01, 0}},
		{2, 0xFF11EE22DD33CC44, []byte{0, 0, 0x44, 0xCC, 0},
			[]byte{0, 0, 0x44, 0xCC, 0x33, 0xDD, 0},
			[]byte{0, 0, 0x44, 0xCC, 0x33, 0xDD, 0x22, 0xEE, 0x11, 0xFF, 0}},
	}

	for _, c := range cases {
		var b []byte
		if littleEndian {
			b := make([]byte, c.i+3)
			*(*uint16)(unsafe.Pointer(&b[c.i])) = uint16(c.v)
			if !reflect.DeepEqual(b, c.b16) {
				t.Errorf("unsafe.Pointer(%d, 0x%X) got %#v want %#v", c.i, c.v, b, c.b16)
			}
		}
		b = make([]byte, c.i+3)
		little.FromUint16(c.i, b, uint16(c.v))
		if !reflect.DeepEqual(b, c.b16) {
			t.Errorf("FromUint16(%d, 0x%X) got %#v want %#v", c.i, c.v, b, c.b16)
		}

		if littleEndian {
			b = make([]byte, c.i+5)
			*(*uint32)(unsafe.Pointer(&b[c.i])) = uint32(c.v)
			if !reflect.DeepEqual(b, c.b32) {
				t.Errorf("unsafe.Pointer(%d, 0x%X) got %#v want %#v", c.i, c.v, b, c.b32)
			}
		}
		b = make([]byte, c.i+5)
		little.FromUint32(c.i, b, uint32(c.v))
		if !reflect.DeepEqual(b, c.b32) {
			t.Errorf("FromUint32(%d, 0x%X) got %#v want %#v", c.i, c.v, b, c.b32)
		}

		if littleEndian {
			b = make([]byte, c.i+9)
			*(*uint64)(unsafe.Pointer(&b[c.i])) = uint64(c.v)
			if !reflect.DeepEqual(b, c.b64) {
				t.Errorf("unsafe.Pointer(%d, 0x%X) got %#v want %#v", c.i, c.v, b, c.b64)
			}
		}
		b = make([]byte, c.i+9)
		little.FromUint64(c.i, b, uint64(c.v))
		if !reflect.DeepEqual(b, c.b64) {
			t.Errorf("FromUint64(%d, 0x%X) got %#v want %#v", c.i, c.v, b, c.b64)
		}
	}
}

func TestFloat64(t *testing.T) {
	f := 1234.5678E9
	b1 := make([]byte, 8)
	*(*float64)(unsafe.Pointer(&b1[0])) = f
	if little.ToFloat64(0, b1) != f {
		t.Errorf("ToFloat64(0, %#v) got %f want %f", b1, little.ToFloat64(0, b1), f)
	}

	b2 := make([]byte, 8)
	little.FromFloat64(0, b2, f)
	if *(*float64)(unsafe.Pointer(&b2[0])) != f {
		t.Errorf("FromFloat64(0, %f) got %#v want %#v", f, b2, b1)
	}
}
