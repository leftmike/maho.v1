package keyval

import (
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

	db, err := badger.OpenManaged(badger.DefaultOptions(dataDir))
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
		var done bool

		item := it.Item()
		err := item.Value(
			func(val []byte) error {
				var err error
				done, err = fn(item.Key(), val, item.Version())
				return err
			})
		if err != nil {
			return err
		}
		if done {
			break
		}

		it.Next()
	}

	return nil
}

func (bkv badgerKV) Update(ver uint64) Updater {
	return badgerUpdater{
		tx: bkv.db.NewTransactionAt(ver, true),
	}
}

func (bu badgerUpdater) Get(key []byte, fn func(val []byte, ver uint64) error) error {
	item, err := bu.tx.Get(key)
	if err != nil {
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
