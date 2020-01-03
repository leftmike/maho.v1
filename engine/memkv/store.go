package memkv

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/google/btree"

	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/localkv"
)

type memKVStore struct {
	mutex sync.RWMutex
	tree  *btree.BTree
}

type memKVTx struct {
	ms       *memKVStore
	tree     *btree.BTree
	writable bool
}

type memKVMapper struct {
	mtx *memKVTx
	mid uint64
}

type memKVWalker struct {
	mtx    *memKVTx
	mid    uint64
	prefix []byte
	keys   [][]byte
	vals   [][]byte
}

type midKeyVal struct {
	mid uint64
	key []byte
	val []byte
}

func (mkv midKeyVal) Less(item btree.Item) bool {
	mkv2 := item.(midKeyVal)
	return mkv.mid < mkv2.mid || (mkv.mid == mkv2.mid && bytes.Compare(mkv.key, mkv2.key) == -1)
}

func OpenStore() *memKVStore {
	return &memKVStore{
		tree: btree.New(16),
	}
}

func (ms *memKVStore) Begin(writable bool) (localkv.Tx, error) {
	if writable {
		ms.mutex.Lock()
		return &memKVTx{
			ms:       ms,
			tree:     ms.tree.Clone(),
			writable: writable,
		}, nil
	}

	ms.mutex.RLock()
	return &memKVTx{
		ms:       ms,
		tree:     ms.tree,
		writable: writable,
	}, nil
}

func (mtx *memKVTx) Map(mid uint64) (localkv.Mapper, error) {
	return &memKVMapper{
		mtx: mtx,
		mid: mid,
	}, nil
}

func (mtx *memKVTx) Commit() error {
	if mtx.ms == nil {
		return errors.New("memkv: transaction already committed or rolled back")
	}

	if mtx.writable {
		mtx.ms.tree = mtx.tree
		mtx.ms.mutex.Unlock()
		mtx.ms = nil
		mtx.tree = nil
		return nil
	}

	mtx.ms.mutex.RUnlock()
	mtx.ms = nil
	return nil
}

func (mtx *memKVTx) Rollback() error {
	if mtx.ms == nil {
		// Rolling back is idempotent.
		return nil
	}

	if mtx.writable {
		mtx.ms.mutex.Unlock()
	} else {
		mtx.ms.mutex.RUnlock()
	}

	mtx.ms = nil
	mtx.tree = nil
	return nil
}

func (mm *memKVMapper) Get(key []byte, vf func(val []byte) error) error {
	item := mm.mtx.tree.Get(midKeyVal{mid: mm.mid, key: key})
	if item == nil {
		return kvrows.ErrKeyNotFound
	}
	return vf(item.(midKeyVal).val)
}

func (mm *memKVMapper) Set(key, val []byte) error {
	if !mm.mtx.writable {
		panic("memkv: set: transaction is not writable")
	}
	mm.mtx.tree.ReplaceOrInsert(midKeyVal{
		mid: mm.mid,
		key: key,
		val: val,
	})
	return nil
}

func (mm *memKVMapper) Walk(prefix []byte) localkv.Walker {
	return &memKVWalker{
		mtx:    mm.mtx,
		mid:    mm.mid,
		prefix: prefix,
	}
}

func (mw *memKVWalker) Close() {
	mw.mtx = nil
	mw.keys = nil
	mw.vals = nil
}

func (mw *memKVWalker) Delete() error {
	if !mw.mtx.writable {
		panic("memkv: set: transaction is not writable")
	}
	if len(mw.keys) == 0 {
		panic("memkv: delete: walker not on a valid key")
	}
	item := mw.mtx.tree.Delete(midKeyVal{mid: mw.mid, key: mw.keys[0]})
	if item == nil {
		panic(fmt.Sprintf("memkv: delete: key not found: %v", mw.keys[0]))
	}
	return nil
}

func (mw *memKVWalker) currentKey() ([]byte, bool) {
	if len(mw.keys) == 0 {
		return nil, false
	}
	return mw.keys[0], true
}

func (mw *memKVWalker) Next() ([]byte, bool) {
	if len(mw.keys) > 0 {
		mw.keys = mw.keys[1:]
		mw.vals = mw.vals[1:]
	}
	return mw.currentKey()
}

func (mw *memKVWalker) itemIterator(item btree.Item) bool {
	mkv := item.(midKeyVal)
	if mkv.mid != mw.mid {
		return false
	}
	if mw.prefix != nil && !bytes.HasPrefix(mkv.key, mw.prefix) {
		return false
	}
	mw.keys = append(mw.keys, mkv.key)
	mw.vals = append(mw.vals, mkv.val)
	return true
}

func (mw *memKVWalker) Rewind() ([]byte, bool) {
	mw.keys = nil
	mw.vals = nil
	mw.mtx.tree.AscendGreaterOrEqual(midKeyVal{mid: mw.mid, key: mw.prefix}, mw.itemIterator)
	return mw.currentKey()
}

func (mw *memKVWalker) Seek(seek []byte) ([]byte, bool) {
	mw.keys = nil
	mw.vals = nil
	mw.mtx.tree.AscendGreaterOrEqual(midKeyVal{mid: mw.mid, key: seek}, mw.itemIterator)
	return mw.currentKey()
}

func (mw *memKVWalker) Value(vf func(val []byte) error) error {
	if len(mw.keys) == 0 {
		panic("memkv: value: walker not on a valid key")
	}
	return vf(mw.vals[0])
}
