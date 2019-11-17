package bbolt

import (
	"bytes"
	"errors"
	"strconv"

	"go.etcd.io/bbolt"

	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/localkv"
)

var (
	errTransactionDone = errors.New("bbolt: transaction done")
	errNoCurrentValue  = errors.New("bbolt: no current value")
)

type bboltStore struct {
	db *bbolt.DB
}

type bboltTx struct {
	tx       *bbolt.Tx
	done     bool
	writable bool
}

type bboltMapper struct {
	bkt *bbolt.Bucket
}

type bboltWalker struct {
	cursor *bbolt.Cursor
	prefix []byte
	value  []byte
}

func OpenStore(path string) (*bboltStore, error) {
	db, err := bbolt.Open(path, 0644, nil)
	if err != nil {
		return nil, err
	}
	return &bboltStore{
		db: db,
	}, nil
}

func (bs *bboltStore) Begin(writable bool) (localkv.Tx, error) {
	tx, err := bs.db.Begin(writable)
	if err != nil {
		return nil, err
	}
	return &bboltTx{
		tx:       tx,
		writable: writable,
	}, nil
}

func (btx *bboltTx) Map(mid uint64) (localkv.Mapper, error) {
	key := []byte(strconv.FormatUint(mid, 10))
	bkt := btx.tx.Bucket(key)
	if bkt == nil && btx.writable {
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
	if btx.done {
		return errTransactionDone
	}
	btx.done = true
	return btx.tx.Commit()
}

func (btx *bboltTx) Rollback() error {
	if btx.done {
		return nil
	}
	btx.done = true
	return btx.tx.Rollback()
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

func (bm *bboltMapper) Walk(prefix []byte) localkv.Walker {
	bw := &bboltWalker{
		prefix: prefix,
	}
	if bm.bkt != nil {
		bw.cursor = bm.bkt.Cursor()
	}
	return bw
}

func (bw *bboltWalker) Close() {
	bw.cursor = nil
	bw.value = nil
}

func (bw *bboltWalker) Delete() error {
	return bw.cursor.Delete()
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
	if bw.cursor == nil {
		return nil, false
	}

	var key []byte
	if bw.prefix == nil {
		key, bw.value = bw.cursor.First()
	} else {
		key, bw.value = bw.cursor.Seek(bw.prefix)
	}
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
	if bw.cursor == nil {
		return nil, false
	}

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
		return errNoCurrentValue
	}
	return vf(bw.value)
}
