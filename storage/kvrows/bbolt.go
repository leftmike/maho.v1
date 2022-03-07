package kvrows

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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
	tx     *bbolt.Tx
	cr     *bbolt.Cursor
	minKey []byte
	maxKey []byte
}

type bboltUpdater struct {
	tx  *bbolt.Tx
	bkt *bbolt.Bucket
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

func (bkv bboltKV) begin(writable bool) (*bbolt.Tx, *bbolt.Bucket, error) {
	tx, err := bkv.db.Begin(writable)
	if err != nil {
		return nil, nil, fmt.Errorf("bbolt: begin failed: %s", err)
	}
	bkt := tx.Bucket(mahoBucket)
	if bkt == nil {
		return nil, nil, errors.New("bbolt: missing maho bucket")
	}
	return tx, bkt, nil
}

func (bkv bboltKV) Iterate(minKey, maxKey []byte) (Iterator, error) {
	tx, bkt, err := bkv.begin(false)
	if err != nil {
		return nil, err
	}

	return &bboltIterator{
		tx:     tx,
		cr:     bkt.Cursor(),
		minKey: minKey,
		maxKey: maxKey,
	}, nil
}

func (bit *bboltIterator) Item(fn func(key, val []byte) error) error {
	var key, val []byte
	if bit.minKey == nil {
		key, val = bit.cr.Next()
	} else {
		key, val = bit.cr.Seek(bit.minKey)
		bit.minKey = nil
	}

	if key == nil || bytes.Compare(bit.maxKey, key) < 0 {
		return io.EOF
	}

	return fn(key, val)
}

func (bit *bboltIterator) Close() {
	if bit.tx != nil {
		bit.tx.Rollback()
	}
}

func (bkv bboltKV) Updater() (Updater, error) {
	tx, bkt, err := bkv.begin(true)
	if err != nil {
		return nil, err
	}
	return bboltUpdater{
		tx:  tx,
		bkt: bkt,
	}, nil
}

func (bu bboltUpdater) Update(key []byte, fn func(val []byte) ([]byte, error)) error {
	val, err := fn(bu.bkt.Get(key))
	if err != nil {
		return err
	}

	if len(val) == 0 {
		err = bu.bkt.Delete(key)
	} else {
		err = bu.bkt.Put(key, val)
	}

	return err
}

func (bu bboltUpdater) Commit(sync bool) error {
	return bu.tx.Commit()
}

func (bu bboltUpdater) Rollback() {
	bu.tx.Rollback()
}
