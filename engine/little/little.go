/*
Convert numbers to and from a byte slice in little endian format.
*/
package little

import (
	"math"
)

func ToUint16(i int, b []byte) uint16 {
	_ = b[i+1] // bounds check hint to compiler; see golang.org/issue/14808
	return uint16(b[i]) | uint16(b[i+1])<<8
}

func FromUint16(i int, b []byte, v uint16) {
	_ = b[i+1] // early bounds check to guarantee safety of writes below
	b[i] = byte(v)
	b[i+1] = byte(v >> 8)
}

func ToUint32(i int, b []byte) uint32 {
	_ = b[i+3] // bounds check hint to compiler; see golang.org/issue/14808
	return uint32(b[i]) | uint32(b[i+1])<<8 | uint32(b[i+2])<<16 | uint32(b[i+3])<<24
}

func FromUint32(i int, b []byte, v uint32) {
	_ = b[i+3] // early bounds check to guarantee safety of writes below
	b[i] = byte(v)
	b[i+1] = byte(v >> 8)
	b[i+2] = byte(v >> 16)
	b[i+3] = byte(v >> 24)
}

func ToUint64(i int, b []byte) uint64 {
	_ = b[i+7] // bounds check hint to compiler; see golang.org/issue/14808
	return uint64(b[i]) | uint64(b[i+1])<<8 | uint64(b[i+2])<<16 | uint64(b[i+3])<<24 |
		uint64(b[i+4])<<32 | uint64(b[i+5])<<40 | uint64(b[i+6])<<48 | uint64(b[i+7])<<56
}

func FromUint64(i int, b []byte, v uint64) {
	_ = b[i+7] // early bounds check to guarantee safety of writes below
	b[i] = byte(v)
	b[i+1] = byte(v >> 8)
	b[i+2] = byte(v >> 16)
	b[i+3] = byte(v >> 24)
	b[i+4] = byte(v >> 32)
	b[i+5] = byte(v >> 40)
	b[i+6] = byte(v >> 48)
	b[i+7] = byte(v >> 56)
}

func ToFloat64(i int, b []byte) float64 {
	return math.Float64frombits(ToUint64(i, b))
}

func FromFloat64(i int, b []byte, v float64) {
	FromUint64(i, b, math.Float64bits(v))
}
