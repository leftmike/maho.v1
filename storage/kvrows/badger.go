package kvrows

import (
	"bytes"
	"io"
	"os"
	"sync"

	"github.com/dgraph-io/badger"
	log "github.com/sirupsen/logrus"
)

type badgerKV struct {
	mutex sync.Mutex
	db    *badger.DB
}

type badgerIterator struct {
	tx     *badger.Txn
	it     *badger.Iterator
	maxKey []byte
}

type badgerUpdater struct {
	kv *badgerKV
	tx *badger.Txn
}

func MakeBadgerKV(dataDir string, logger *log.Logger) (KV, error) {
	os.MkdirAll(dataDir, 0755)

	opts := badger.DefaultOptions(dataDir)
	opts = opts.WithBypassLockGuard(true)
	opts = opts.WithLogger(logger)
	opts = opts.WithSyncWrites(false)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &badgerKV{
		db: db,
	}, nil
}

func (bkv *badgerKV) Iterate(minKey, maxKey []byte) (Iterator, error) {
	tx := bkv.db.NewTransaction(false)
	it := tx.NewIterator(badger.DefaultIteratorOptions)
	it.Seek(minKey)

	return badgerIterator{
		tx:     tx,
		it:     it,
		maxKey: maxKey,
	}, nil
}

func (bit badgerIterator) Item(fn func(key, val []byte) error) error {
	if !bit.it.Valid() {
		return io.EOF
	}

	item := bit.it.Item()
	err := item.Value(
		func(val []byte) error {
			key := item.Key()
			if bytes.Compare(bit.maxKey, key) < 0 {
				return io.EOF
			}
			return fn(key, val)
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

func (bkv *badgerKV) Updater() (Updater, error) {
	bkv.mutex.Lock()

	return badgerUpdater{
		kv: bkv,
		tx: bkv.db.NewTransaction(true),
	}, nil
}

func (bu badgerUpdater) Update(key []byte, fn func(val []byte) ([]byte, error)) error {
	item, err := bu.tx.Get(key)

	var newVal []byte
	if err == badger.ErrKeyNotFound {
		newVal, err = fn(nil)
	} else if err != nil {
		return err
	} else {
		err = item.Value(
			func(val []byte) error {
				var err error
				newVal, err = fn(val)
				return err
			})
	}

	if err != nil {
		return err
	}

	if len(newVal) == 0 {
		err = bu.tx.Delete(key)
	} else {
		err = bu.tx.Set(key, newVal)
	}

	return err
}

func (bu badgerUpdater) Commit(sync bool) error {
	err := bu.tx.Commit()
	bu.kv.mutex.Unlock()
	return err
}

func (bu badgerUpdater) Rollback() {
	bu.tx.Discard()
	bu.kv.mutex.Unlock()
}
