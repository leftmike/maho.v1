package kvrows

import (
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
	tx   *bbolt.Tx
	cr   *bbolt.Cursor
	key  []byte
	next bool
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

func (bkv bboltKV) Iterate(key []byte) (Iterator, error) {
	tx, bkt, err := bkv.begin(false)
	if err != nil {
		return nil, err
	}

	return &bboltIterator{
		tx:  tx,
		cr:  bkt.Cursor(),
		key: append(make([]byte, 0, len(key)), key...),
	}, nil
}

func (bit *bboltIterator) Item(fn func(key, val []byte) error) error {
	var key, val []byte
	if bit.next {
		key, val = bit.cr.Next()
	} else {
		key, val = bit.cr.Seek(bit.key)
		bit.next = true
		bit.key = nil
	}

	if key == nil {
		return io.EOF
	}

	return fn(key, val)
}

func (bit *bboltIterator) Close() {
	if bit.tx != nil {
		bit.tx.Rollback()
	}
}

func (bkv bboltKV) Update(key []byte, fn func(val []byte) ([]byte, error)) error {
	tx, bkt, err := bkv.begin(true)
	if err != nil {
		return err
	}

	val, err := fn(bkt.Get(key))
	if err != nil {
		tx.Rollback()
		return err
	}

	if len(val) == 0 {
		err = bkt.Delete(key)
	} else {
		err = bkt.Put(key, val)
	}

	if err != nil {
		tx.Rollback()
		return err
	}

	tx.Commit()
	return nil
}

func (bkv bboltKV) Get(key []byte, fn func(val []byte) error) error {
	tx, bkt, err := bkv.begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	val := bkt.Get(key)
	if val == nil {
		return io.EOF
	}
	return fn(val)
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

func (bu bboltUpdater) Iterate(key []byte) (Iterator, error) {
	return &bboltIterator{
		cr:  bu.bkt.Cursor(),
		key: append(make([]byte, 0, len(key)), key...),
	}, nil
}

func (bu bboltUpdater) Get(key []byte, fn func(val []byte) error) error {
	val := bu.bkt.Get(key)
	if val == nil {
		return io.EOF
	}
	return fn(val)
}

func (bu bboltUpdater) Set(key, val []byte) error {
	return bu.bkt.Put(key, val)
}

func (bu bboltUpdater) Commit(sync bool) error {
	return bu.tx.Commit()
}

func (bu bboltUpdater) Rollback() {
	bu.tx.Rollback()
}
