package badger

import (
	"os"

	"github.com/dgraph-io/badger"

	"github.com/leftmike/maho/engine/kvrows"
)

type badgerStore struct {
	db *badger.DB
}

type badgerTx struct {
	tx *badger.Txn
}

type badgerMapper struct {
	tx     *badgerTx
	prefix []byte
}

type badgerWalker struct {
	it     *badger.Iterator
	prefix []byte
}

func openStore(path string) (*badgerStore, error) {
	os.MkdirAll(path, 0755)
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return &badgerStore{
		db: db,
	}, nil
}

func (bs *badgerStore) Begin() (kvrows.Tx, error) {
	return &badgerTx{
		tx: bs.db.NewTransaction(true),
	}, nil
}

func (btx *badgerTx) Map(mid uint64) (kvrows.Mapper, error) {
	return &badgerMapper{
		tx:     btx,
		prefix: kvrows.EncodeUint64(mid),
	}, nil
}

func (btx *badgerTx) Commit() error {
	return btx.tx.Commit()
}

func (btx *badgerTx) Rollback() error {
	btx.tx.Discard()
	return nil
}

func (bm *badgerMapper) addPrefix(key []byte) []byte {
	return append(bm.prefix, key...)
}

func (bm *badgerMapper) Delete(key []byte) error {
	return bm.tx.tx.Delete(bm.addPrefix(key))
}

func (bm *badgerMapper) Get(key []byte, vf func(val []byte) error) error {
	item, err := bm.tx.tx.Get(bm.addPrefix(key))
	if err == badger.ErrKeyNotFound {
		return kvrows.ErrKeyNotFound
	} else if err != nil {
		return err
	}
	return item.Value(vf)
}

func (bm *badgerMapper) Set(key, val []byte) error {
	return bm.tx.tx.Set(bm.addPrefix(key), val)
}

func (bm *badgerMapper) Walk(prefix []byte) kvrows.Walker {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	return &badgerWalker{
		it:     bm.tx.tx.NewIterator(opts),
		prefix: bm.addPrefix(prefix),
	}
}

func (bw *badgerWalker) Close() {
	bw.it.Close()
}

func (bw *badgerWalker) currentKey() ([]byte, bool) {
	if bw.prefix == nil && !bw.it.Valid() {
		return nil, false
	} else if bw.prefix != nil && !bw.it.ValidForPrefix(bw.prefix) {
		return nil, false
	}
	return bw.it.Item().Key(), true
}

func (bw *badgerWalker) Next() ([]byte, bool) {
	bw.it.Next()
	return bw.currentKey()
}

func (bw *badgerWalker) Rewind() ([]byte, bool) {
	bw.it.Rewind()
	return bw.currentKey()
}

func (bw *badgerWalker) Seek(seek []byte) ([]byte, bool) {
	bw.it.Seek(seek)
	return bw.currentKey()
}

func (bw *badgerWalker) Value(vf func(val []byte) error) error {
	if !bw.it.Valid() {
		return kvrows.ErrMissingValue
	}
	return bw.it.Item().Value(vf)
}
