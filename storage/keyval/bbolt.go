package keyval

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"

	"go.etcd.io/bbolt"
)

var (
	mahoBucket = []byte{'m', 'a', 'h', 'o'}
)

type bboltKV struct {
	db *bbolt.DB
}

type bboltIterator struct {
	ver uint64
	tx  *bbolt.Tx
	cr  *bbolt.Cursor
	key []byte
	val []byte
}

type bboltUpdater struct {
	tx  *bbolt.Tx
	bkt *bbolt.Bucket
	ver uint64
}

func MakeBBoltKV(dataDir string) (KV, error) {
	db, err := bbolt.Open(filepath.Join(dataDir, "maho.bbolt"), 0644, nil)
	if err != nil {
		return nil, err
	}
	// Dangerous, but about 100x faster.
	db.NoFreelistSync = true
	db.NoSync = true

	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	if tx.Bucket(mahoBucket) == nil {
		_, err = tx.CreateBucket(mahoBucket)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		err = tx.Commit()
		if err != nil {
			return nil, err
		}
	} else {
		tx.Rollback()
	}

	return bboltKV{
		db: db,
	}, nil
}

func (bkv bboltKV) Iterate(ver uint64, key []byte) (Iterator, error) {
	tx, err := bkv.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("bbolt: begin failed: %s", err)
	}
	bkt := tx.Bucket(mahoBucket)
	if bkt == nil {
		return nil, errors.New("bbolt: missing maho bucket")
	}
	cr := bkt.Cursor()
	key, val := cr.Seek(key)

	return &bboltIterator{
		ver: ver,
		tx:  tx,
		cr:  cr,
		key: key,
		val: val,
	}, nil
}

func encodeKey(key []byte, ver uint64) []byte {
	buf := append(make([]byte, 0, len(key)+8), key...)
	ver = ^ver
	return append(buf, byte(ver>>56), byte(ver>>48), byte(ver>>40), byte(ver>>32),
		byte(ver>>24), byte(ver>>16), byte(ver>>8), byte(ver))
}

func decodeKey(buf []byte) ([]byte, uint64) {
	if len(buf) < 8 {
		panic(fmt.Sprintf("bbolt: decode key too short: %v", buf))
	}

	return buf[:len(buf)-8], ^binary.BigEndian.Uint64(buf[len(buf)-8:])
}

func (bit *bboltIterator) Item(fn func(key, val []byte, ver uint64) error) error {
	for bit.key != nil {
		key, ver := decodeKey(bit.key)
		if ver <= bit.ver {
			val := bit.val

			for {
				bit.key, bit.val = bit.cr.Next()
				if bit.key == nil {
					break
				}
				k, _ := decodeKey(bit.key)
				if !bytes.Equal(k, key) {
					break
				}
			}

			return fn(key, val, ver)
		}

		bit.key, bit.val = bit.cr.Next()
	}

	return io.EOF
}

func (bit *bboltIterator) Close() {
	bit.tx.Rollback()
}

func (bkv bboltKV) GetAt(ver uint64, key []byte, fn func(val []byte, ver uint64) error) error {
	it, err := bkv.Iterate(ver, key)
	if err != nil {
		return err
	}
	defer it.Close()

	return it.Item(
		func(itemKey, val []byte, ver uint64) error {
			if !bytes.Equal(itemKey, key) {
				return io.EOF
			}
			return fn(val, ver)
		})
}

func (bkv bboltKV) Update(ver uint64) (Updater, error) {
	tx, err := bkv.db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("bbolt: begin failed: %s", err)
	}
	bkt := tx.Bucket(mahoBucket)
	if bkt == nil {
		return nil, errors.New("bbolt: missing maho bucket")
	}
	return bboltUpdater{
		tx:  tx,
		bkt: bkt,
		ver: ver,
	}, nil
}

func (bu bboltUpdater) Get(key []byte, fn func(val []byte, ver uint64) error) error {
	cr := bu.bkt.Cursor()
	kbuf, val := cr.Seek(encodeKey(key, math.MaxUint64))
	if kbuf == nil {
		return io.EOF
	}
	found, ver := decodeKey(kbuf)
	if !bytes.Equal(found, key) {
		return io.EOF
	}
	return fn(val, ver)
}

func (bu bboltUpdater) Set(key, val []byte) error {
	return bu.bkt.Put(encodeKey(key, bu.ver), val)
}

func (bu bboltUpdater) Commit() error {
	return bu.tx.Commit()
}

func (bu bboltUpdater) Rollback() {
	bu.tx.Rollback()
}
