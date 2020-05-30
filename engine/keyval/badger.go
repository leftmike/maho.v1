package keyval

import (
	"io"
	"os"

	"github.com/dgraph-io/badger"
)

type badgerKV struct {
	db *badger.DB
}

type badgerUpdater struct {
	tx *badger.Txn
}

func MakeBadgerKV(dataDir string) (KV, error) {
	os.MkdirAll(dataDir, 0755)

	db, err := badger.OpenManaged(badger.DefaultOptions(dataDir).WithBypassLockGuard(true))
	if err != nil {
		return nil, err
	}
	return badgerKV{
		db: db,
	}, nil
}

func (bkv badgerKV) IterateAt(ver uint64, key []byte,
	fn func(key, val []byte, ver uint64) (bool, error)) error {

	tx := bkv.db.NewTransactionAt(ver, false)
	defer tx.Discard()

	it := tx.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	it.Seek(key)
	for it.Valid() {
		var more bool

		item := it.Item()
		err := item.Value(
			func(val []byte) error {
				var err error
				more, err = fn(item.Key(), val, item.Version())
				return err
			})
		if err != nil {
			return err
		}
		if !more {
			break
		}

		it.Next()
	}

	return nil
}

func (bkv badgerKV) GetAt(ver uint64, key []byte, fn func(val []byte, ver uint64) error) error {
	tx := bkv.db.NewTransactionAt(ver, false)
	defer tx.Discard()

	return get(tx, key, fn)
}

func (bkv badgerKV) Update(ver uint64) Updater {
	return badgerUpdater{
		tx: bkv.db.NewTransactionAt(ver, true),
	}
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

func (bu badgerUpdater) CommitAt(ver uint64) error {
	return bu.tx.CommitAt(ver, nil)
}

func (bu badgerUpdater) Rollback() {
	bu.tx.Discard()
}
