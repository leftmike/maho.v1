package badger

import (
	"os"

	"github.com/dgraph-io/badger"

	"github.com/leftmike/maho/engine/kv"
)

type Engine struct{}

type database struct {
	db *badger.DB
}

type readTx struct {
	tx *badger.Txn
}

type writeTx struct {
	readTx
}

type iterator struct {
	it     *badger.Iterator
	prefix []byte
}

func (Engine) Open(path string) (kv.DB, error) {
	os.MkdirAll(path, 0755)
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return database{db}, nil
}

func (db database) ReadTx() (kv.ReadTx, error) {
	return readTx{db.db.NewTransaction(false)}, nil
}

func (db database) WriteTx() (kv.WriteTx, error) {
	return writeTx{readTx{db.db.NewTransaction(true)}}, nil
}

func (db database) Close() error {
	return db.db.Close()
}

func (rtx readTx) Discard() {
	rtx.tx.Discard()
}

func (rtx readTx) Get(key []byte, vf func(val []byte) error) error {
	item, err := rtx.tx.Get(key)
	if err == badger.ErrKeyNotFound {
		return kv.ErrKeyNotFound
	} else if err != nil {
		return err
	}
	return item.Value(vf)
}

func (rtx readTx) GetValue(key []byte) ([]byte, error) {
	item, err := rtx.tx.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, kv.ErrKeyNotFound
	} else if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (rtx readTx) Iterate(prefix []byte) kv.Iterator {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := rtx.tx.NewIterator(opts)
	return &iterator{
		it:     it,
		prefix: prefix,
	}
}

func (wtx writeTx) Commit() error {
	return wtx.tx.Commit()
}

func (wtx writeTx) Delete(key []byte) error {
	return wtx.tx.Delete(key)
}

func (wtx writeTx) Set(key []byte, val []byte) error {
	return wtx.tx.Set(key, val)
}

func (it *iterator) Close() {
	it.it.Close()
}

func (it *iterator) Key() []byte {
	if !it.it.ValidForPrefix(it.prefix) {
		return nil
	}
	return it.it.Item().Key()
}

func (it *iterator) KeyCopy() []byte {
	if !it.it.ValidForPrefix(it.prefix) {
		return nil
	}
	return it.it.Item().KeyCopy(nil)
}

func (it *iterator) Next() {
	it.it.Next()
}

func (it *iterator) Rewind() {
	it.it.Seek(it.prefix)
}

func (it *iterator) Seek(key []byte) {
	it.it.Seek(key)
}

func (it *iterator) Valid() bool {
	return it.it.ValidForPrefix(it.prefix)
}

func (it *iterator) Value(vf func(val []byte) error) error {
	return it.it.Item().Value(vf)
}
