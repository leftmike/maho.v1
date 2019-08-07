package bbolt

import (
	"bytes"
	"strconv"

	"go.etcd.io/bbolt"

	"github.com/leftmike/maho/engine/kvrows"
)

type bboltStore struct {
	db *bbolt.DB
}

type bboltTx struct {
	tx *bbolt.Tx
}

type bboltMapper struct {
	bkt *bbolt.Bucket
}

type bboltWalker struct {
	cursor *bbolt.Cursor
	prefix []byte
	value  []byte
}

func openStore(path string) (*bboltStore, error) {
	db, err := bbolt.Open(path, 0644, nil)
	if err != nil {
		return nil, err
	}
	return &bboltStore{
		db: db,
	}, nil
}

func (bs *bboltStore) Begin() (kvrows.Tx, error) {
	tx, err := bs.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &bboltTx{
		tx: tx,
	}, nil
}

func (btx *bboltTx) Map(mid uint64) (kvrows.Mapper, error) {
	key := []byte(strconv.FormatUint(mid, 10))
	bkt := btx.tx.Bucket(key)
	if bkt == nil {
		var err error
		bkt, err = btx.tx.CreateBucket(key)
		if err != nil {
			return nil, err
		}
	}
	return &bboltMapper{
		bkt: bkt,
	}, nil
}

func (btx *bboltTx) Commit() error {
	return btx.tx.Commit()
}

func (btx *bboltTx) Rollback() error {
	return btx.tx.Rollback()
}

func (bm *bboltMapper) Delete(key []byte) error {
	return bm.bkt.Delete(key)
}

func (bm *bboltMapper) Get(key []byte, vf func(val []byte) error) error {
	val := bm.bkt.Get(key)
	if val == nil {
		return kvrows.ErrKeyNotFound
	}
	return vf(val)
}

func (bm *bboltMapper) Set(key, val []byte) error {
	return bm.bkt.Put(key, val)
}

func (bm *bboltMapper) Walk(prefix []byte) kvrows.Walker {
	return &bboltWalker{
		cursor: bm.bkt.Cursor(),
		prefix: prefix,
	}
}

func (bw *bboltWalker) Close() {
	bw.cursor = nil
	bw.value = nil
}

func (bw *bboltWalker) Next() ([]byte, bool) {
	var key []byte
	key, bw.value = bw.cursor.Next()
	if key == nil {
		return nil, false
	}
	if bw.prefix != nil && !bytes.HasPrefix(key, bw.prefix) {
		bw.value = nil
		return nil, false
	}
	return key, true
}

func (bw *bboltWalker) Rewind() ([]byte, bool) {
	var key []byte
	key, bw.value = bw.cursor.First()
	if key == nil {
		return nil, false
	}
	if bw.prefix != nil && !bytes.HasPrefix(key, bw.prefix) {
		bw.value = nil
		return nil, false
	}
	return key, true
}

func (bw *bboltWalker) Seek(seek []byte) ([]byte, bool) {
	var key []byte
	key, bw.value = bw.cursor.Seek(seek)
	if key == nil {
		return nil, false
	}
	if bw.prefix != nil && !bytes.HasPrefix(key, bw.prefix) {
		bw.value = nil
		return nil, false
	}
	return key, true
}

func (bw *bboltWalker) Value(vf func(val []byte) error) error {
	if bw.value == nil {
		return kvrows.ErrMissingValue
	}
	return vf(bw.value)
}
