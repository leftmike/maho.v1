package kvrows

import (
	"io"
	"os"
	"sync"

	"github.com/dgraph-io/badger"
)

type badgerKV struct {
	mutex sync.Mutex
	db    *badger.DB
}

type badgerIterator struct {
	tx *badger.Txn
	it *badger.Iterator
}

type badgerUpdater struct {
	kv *badgerKV
	tx *badger.Txn
}

func MakeBadgerKV(dataDir string) (KV, error) {
	os.MkdirAll(dataDir, 0755)

	db, err := badger.Open(badger.DefaultOptions(dataDir).WithBypassLockGuard(true))
	if err != nil {
		return nil, err
	}
	return &badgerKV{
		db: db,
	}, nil
}

func (bkv *badgerKV) Iterate(key []byte) (Iterator, error) {
	tx := bkv.db.NewTransaction(false)
	it := tx.NewIterator(badger.DefaultIteratorOptions)
	it.Seek(key)

	return badgerIterator{
		tx: tx,
		it: it,
	}, nil
}

func (bit badgerIterator) Item(fn func(key, val []byte) error) error {
	if !bit.it.Valid() {
		return io.EOF
	}

	item := bit.it.Item()
	err := item.Value(
		func(val []byte) error {
			return fn(item.Key(), val)
		})
	if err != nil {
		return err
	}

	bit.it.Next()
	return nil
}

func (bit badgerIterator) Close() {
	bit.it.Close()
	if bit.tx != nil {
		bit.tx.Discard()
	}
}

func (bkv *badgerKV) Get(key []byte, fn func(val []byte) error) error {
	tx := bkv.db.NewTransaction(false)
	defer tx.Discard()

	return get(tx, key, fn)
}

func (bkv *badgerKV) Update() (Updater, error) {
	bkv.mutex.Lock()

	return badgerUpdater{
		kv: bkv,
		tx: bkv.db.NewTransaction(true),
	}, nil
}

func (bu badgerUpdater) Iterate(key []byte) (Iterator, error) {
	it := bu.tx.NewIterator(badger.DefaultIteratorOptions)
	it.Seek(key)

	return badgerIterator{
		it: it,
	}, nil
}

func (bu badgerUpdater) Get(key []byte, fn func(val []byte) error) error {
	return get(bu.tx, key, fn)
}

func get(tx *badger.Txn, key []byte, fn func(val []byte) error) error {
	item, err := tx.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return io.EOF
		}
		return err
	}
	return item.Value(
		func(val []byte) error {
			return fn(val)
		})
}

func (bu badgerUpdater) Set(key, val []byte) error {
	return bu.tx.Set(key, val)
}

func (bu badgerUpdater) Commit() error {
	err := bu.tx.Commit()
	bu.kv.mutex.Unlock()
	return err
}

func (bu badgerUpdater) Rollback() {
	bu.tx.Discard()
	bu.kv.mutex.Unlock()
}
