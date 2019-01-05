package bbolt

import (
	"bytes"
	"fmt"

	"go.etcd.io/bbolt"

	"github.com/leftmike/maho/engine/kv"
)

var (
	bucketName = []byte("kv")
)

type Engine struct{}

type database struct {
	db *bbolt.DB
}

type readTx struct {
	tx   *bbolt.Tx
	done bool
}

type writeTx struct {
	readTx
}

type iterator struct {
	cursor *bbolt.Cursor
	prefix []byte
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
	if rtx.done {
		return
	}
	rtx.done = true
	err := rtx.tx.Rollback()
	if err != nil {
		panic(fmt.Sprintf("bbolt.Rollback() failed"))
	}
}

func (rtx *readTx) Get(key []byte, vf func(val []byte) error) error {
	val, err := rtx.GetValue(key)
	if err != nil {
		return err
	}
	return vf(val)
}

func getBucket(tx *bbolt.Tx) *bbolt.Bucket {
	return tx.Bucket(bucketName)
}

func (rtx *readTx) GetValue(key []byte) ([]byte, error) {
	bkt := getBucket(rtx.tx)
	if bkt == nil {
		return nil, kv.ErrKeyNotFound
	}
	val := bkt.Get(key)
	if val == nil {
		return nil, kv.ErrKeyNotFound
	}
	return val, nil
}

func (rtx *readTx) Iterate(prefix []byte) kv.Iterator {
	var cursor *bbolt.Cursor
	bkt := getBucket(rtx.tx)
	if bkt == nil {
		cursor = rtx.tx.Cursor()
	} else {
		cursor = bkt.Cursor()
	}
	return &iterator{
		cursor: cursor,
		prefix: prefix,
	}
}

func (wtx *writeTx) Commit() error {
	if wtx.done {
		return nil
	}
	wtx.done = true
	return wtx.tx.Commit()
}

func (wtx *writeTx) Delete(key []byte) error {
	bkt := getBucket(wtx.tx)
	if bkt == nil {
		return kv.ErrKeyNotFound
	}
	return bkt.Delete(key)
}

func (wtx *writeTx) Set(key []byte, val []byte) error {
	bkt, err := wtx.tx.CreateBucketIfNotExists(bucketName)
	if err != nil {
		return err
	}
	return bkt.Put(key, val)
}

func (it *iterator) Close() {
	// Nothing.
}

func (it *iterator) Key() []byte {
	return it.key
}

func (it *iterator) KeyCopy() []byte {
	return it.key
}

func (it *iterator) setKeyVal(key, val []byte) {
	if key != nil && bytes.HasPrefix(key, it.prefix) {
		it.key = key
		it.val = val
	} else {
		it.key = nil
	}
}

func (it *iterator) Next() {
	it.setKeyVal(it.cursor.Next())
}

func (it *iterator) Rewind() {
	it.setKeyVal(it.cursor.Seek(it.prefix))
}

func (it *iterator) Seek(key []byte) {
	it.setKeyVal(it.cursor.Seek(key))
}

func (it *iterator) Valid() bool {
	return it.key != nil
}

func (it *iterator) Value(vf func(val []byte) error) error {
	return vf(it.val)
}
