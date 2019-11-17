package badger

import (
	"os"

	"github.com/dgraph-io/badger"

	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/localkv"
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
	tx     *badgerTx
	prefix []byte
}

func OpenStore(path string) (*badgerStore, error) {
	os.MkdirAll(path, 0755)
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return &badgerStore{
		db: db,
	}, nil
}

func (bs *badgerStore) Begin(writable bool) (localkv.Tx, error) {
	return &badgerTx{
		tx: bs.db.NewTransaction(writable),
	}, nil
}

func (btx *badgerTx) Map(mid uint64) (localkv.Mapper, error) {
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

func (bm *badgerMapper) Walk(prefix []byte) localkv.Walker {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = bm.addPrefix(prefix)
	return &badgerWalker{
		it:     bm.tx.tx.NewIterator(opts),
		tx:     bm.tx,
		prefix: opts.Prefix,
	}
}

func (bw *badgerWalker) Close() {
	if bw.it != nil {
		bw.it.Close()
		bw.it = nil
	}
}

func (bw *badgerWalker) currentKey() ([]byte, bool) {
	if bw.prefix == nil && !bw.it.Valid() {
		return nil, false
	} else if bw.prefix != nil && !bw.it.ValidForPrefix(bw.prefix) {
		return nil, false
	}
	key := bw.it.Item().Key()
	if len(key) < 8 {
		return nil, false
	}
	return key[8:], true
}

func (bw *badgerWalker) Delete() error {
	if !bw.it.Valid() {
		return kvrows.ErrKeyNotFound
	}
	return bw.tx.tx.Delete(bw.it.Item().Key())
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
		return kvrows.ErrKeyNotFound
	}
	return bw.it.Item().Value(vf)
}
