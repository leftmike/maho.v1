package badger

import (
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

func (rtx readTx) Commit() error {
	return rtx.tx.Commit()
}

func buildKey(key1 []byte, key2 []byte, key3 []byte) []byte {
	key := make([]byte, 0, len(key1)+len(key2)+len(key3)+2)
	key = append(key, key1...)
	key = append(key, '/')
	key = append(key, key2...)
	key = append(key, '/')
	key = append(key, key3...)
	return key
}

func (rtx readTx) Get(key1 []byte, key2 []byte, key3 []byte, vf func(val []byte) error) error {
	item, err := rtx.tx.Get(buildKey(key1, key2, key3))
	if err == badger.ErrKeyNotFound {
		return kv.ErrKeyNotFound
	} else if err != nil {
		return err
	}
	return item.Value(vf)
}

func (rtx readTx) Iterate(key1 []byte, key2 []byte) (kv.Iterator, error) {
	prefix := buildKey(key1, key2, []byte{})
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	return &iterator{
		it:     rtx.tx.NewIterator(opts),
		prefix: prefix,
	}, nil
}

func (wtx writeTx) Delete(key1 []byte, key2 []byte, key3 []byte) error {
	return wtx.tx.Delete(buildKey(key1, key2, key3))
}

func (wtx writeTx) DeleteAll(key1 []byte, key2 []byte) error {
	prefix := buildKey(key1, key2, []byte{})
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := wtx.tx.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		err := wtx.tx.Delete(it.Item().KeyCopy(nil))
		if err != nil {
			return err
		}
	}
	return nil
}

func (wtx writeTx) Set(key1 []byte, key2 []byte, key3 []byte, val []byte) error {
	return wtx.tx.Set(buildKey(key1, key2, key3), val)
}

func (wtx writeTx) Rollback() error {
	wtx.tx.Discard()
	return nil
}

func (it *iterator) Close() {
	it.it.Close()
}

func (it *iterator) Key() []byte {
	if !it.it.ValidForPrefix(it.prefix) {
		return nil
	}
	return it.it.Item().Key()[len(it.prefix):]
}

func (it *iterator) Next() {
	it.it.Next()
}

func (it *iterator) Rewind() {
	it.it.Rewind()
}

func (it *iterator) Seek(key []byte) {
	fullKey := make([]byte, 0, len(it.prefix)+len(key))
	fullKey = append(fullKey, it.prefix...)
	fullKey = append(fullKey, key...)
	it.it.Seek(fullKey)
}

func (it *iterator) Valid() bool {
	return it.it.ValidForPrefix(it.prefix)
}

func (it *iterator) Value(vf func(val []byte) error) error {
	return it.it.Item().Value(vf)
}
