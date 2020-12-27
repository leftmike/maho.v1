package keyval

import (
	"io"
	"math"
	"os"

	"github.com/dgraph-io/badger"
	log "github.com/sirupsen/logrus"
)

type badgerKV struct {
	db *badger.DB
}

type badgerIterator struct {
	tx *badger.Txn
	it *badger.Iterator
}

type badgerUpdater struct {
	tx  *badger.Txn
	ver uint64
}

func MakeBadgerKV(dataDir string, logger *log.Logger) (KV, error) {
	os.MkdirAll(dataDir, 0755)

	opts := badger.DefaultOptions(dataDir)
	opts = opts.WithBypassLockGuard(true)
	opts = opts.WithLogger(logger)
	db, err := badger.OpenManaged(opts)
	if err != nil {
		return nil, err
	}
	return badgerKV{
		db: db,
	}, nil
}

func (bkv badgerKV) Iterate(ver uint64, key []byte) (Iterator, error) {
	tx := bkv.db.NewTransactionAt(ver, false)
	it := tx.NewIterator(badger.DefaultIteratorOptions)
	it.Seek(key)

	return badgerIterator{
		tx: tx,
		it: it,
	}, nil
}

func (bit badgerIterator) Item(fn func(key, val []byte, ver uint64) error) error {
	if !bit.it.Valid() {
		return io.EOF
	}

	item := bit.it.Item()
	err := item.Value(
		func(val []byte) error {
			return fn(item.Key(), val, item.Version())
		})
	if err != nil {
		return err
	}

	bit.it.Next()
	return nil
}

func (bit badgerIterator) Close() {
	bit.it.Close()
	bit.tx.Discard()
}

func (bkv badgerKV) GetAt(ver uint64, key []byte, fn func(val []byte, ver uint64) error) error {
	tx := bkv.db.NewTransactionAt(ver, false)
	defer tx.Discard()

	return get(tx, key, fn)
}

func (bkv badgerKV) Update(ver uint64) (Updater, error) {
	return badgerUpdater{
		tx:  bkv.db.NewTransactionAt(math.MaxUint64, true),
		ver: ver,
	}, nil
}

func (bu badgerUpdater) Get(key []byte, fn func(val []byte, ver uint64) error) error {
	return get(bu.tx, key, fn)
}

func get(tx *badger.Txn, key []byte, fn func(val []byte, ver uint64) error) error {
	item, err := tx.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return io.EOF
		}
		return err
	}
	return item.Value(
		func(val []byte) error {
			return fn(val, item.Version())
		})
}

func (bu badgerUpdater) Set(key, val []byte) error {
	return bu.tx.Set(key, val)
}

func (bu badgerUpdater) Commit() error {
	return bu.tx.CommitAt(bu.ver, nil)
}

func (bu badgerUpdater) Rollback() {
	bu.tx.Discard()
}
