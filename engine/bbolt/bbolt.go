package bbolt

import (
	"fmt"

	"go.etcd.io/bbolt"

	"github.com/leftmike/maho/engine/kv"
)

type Engine struct{}

type database struct {
	db *bbolt.DB
}

type readTx struct {
	tx        *bbolt.Tx
	discarded bool
}

type writeTx struct {
	readTx
}

type iterator struct {
	cursor *bbolt.Cursor
	key    []byte
	val    []byte
}

func (Engine) Open(path string) (kv.DB, error) {
	db, err := bbolt.Open(path, 0644, nil)
	if err != nil {
		return nil, err
	}
	return database{db}, nil
}

func (db database) ReadTx() (kv.ReadTx, error) {
	tx, err := db.db.Begin(false)
	if err != nil {
		return nil, err
	}
	return &readTx{tx: tx}, nil
}

func (db database) WriteTx() (kv.WriteTx, error) {
	tx, err := db.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &writeTx{readTx{tx: tx}}, nil
}

func (db database) Close() error {
	return db.db.Close()
}

func (rtx *readTx) Discard() {
	if !rtx.discarded {
		rtx.discarded = true
		err := rtx.tx.Rollback()
		if err != nil {
			panic(fmt.Sprintf("bbolt.Rollback() failed"))
		}
	}
}

func getBucket(tx *bbolt.Tx, key1 string, key2 string) *bbolt.Bucket {
	bkt := tx.Bucket([]byte(key1))
	if bkt == nil {
		return nil
	}
	return bkt.Bucket([]byte(key2))
}

func (rtx *readTx) Get(key1 string, key2 string, key3 []byte, vf func(val []byte) error) error {
	val, err := rtx.GetValue(key1, key2, key3)
	if err != nil {
		return err
	}
	return vf(val)
}

func (rtx *readTx) GetValue(key1 string, key2 string, key3 []byte) ([]byte, error) {
	bkt := getBucket(rtx.tx, key1, key2)
	if bkt == nil {
		return nil, kv.ErrKeyNotFound
	}
	val := bkt.Get(key3)
	if val == nil {
		return nil, kv.ErrKeyNotFound
	}
	return val, nil
}

func (rtx *readTx) Iterate(key1 string, key2 string) (kv.Iterator, error) {
	bkt := getBucket(rtx.tx, key1, key2)
	if bkt == nil {
		return nil, kv.ErrKeyNotFound
	}
	return &iterator{
		cursor: bkt.Cursor(),
	}, nil
}

func (wtx *writeTx) Commit() error {
	if wtx.discarded {
		return nil
	}
	return wtx.tx.Commit()
}

func (wtx *writeTx) Delete(key1 string, key2 string, key3 []byte) error {
	bkt := getBucket(wtx.tx, key1, key2)
	if bkt == nil {
		return kv.ErrKeyNotFound
	}
	return bkt.Delete(key3)
}

func (wtx *writeTx) DeleteAll(key1 string, key2 string) error {
	bkt := wtx.tx.Bucket([]byte(key1))
	if bkt == nil {
		return kv.ErrKeyNotFound
	}
	return bkt.DeleteBucket([]byte(key2))
}

func (wtx *writeTx) Set(key1 string, key2 string, key3 []byte, val []byte) error {
	bkt, err := wtx.tx.CreateBucketIfNotExists([]byte(key1))
	if err != nil {
		return err
	}
	bkt, err = bkt.CreateBucketIfNotExists([]byte(key2))
	if err != nil {
		return err
	}
	return bkt.Put(key3, val)
}

func (it *iterator) Close() {
	// Nothing.
}

func (it *iterator) Key() []byte {
	return it.key
}

func (it *iterator) Next() {
	it.key, it.val = it.cursor.Next()
}

func (it *iterator) Rewind() {
	it.key, it.val = it.cursor.First()
}

func (it *iterator) Seek(key []byte) {
	it.key, it.val = it.cursor.Seek(key)
}

func (it *iterator) Valid() bool {
	return it.key != nil && it.val != nil
}

func (it *iterator) Value(vf func(val []byte) error) error {
	return vf(it.val)
}
