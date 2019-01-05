package kvrows

import (
	"encoding/binary"

	"github.com/leftmike/maho/engine/kv"
)

func setString(wtx kv.WriteTx, key string, val string) error {
	return wtx.Set([]byte(key), []byte(val))
}

func setUInt32(wtx kv.WriteTx, key string, val uint32) error {
	b := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(b, val)
	return wtx.Set([]byte(key), b)
}
