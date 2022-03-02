package kvrows

import (
	"bytes"
	"io"
	"sync"

	"github.com/google/btree"
)

type btreeKV struct {
	treeMutex   sync.Mutex
	updateMutex sync.Mutex
	tree        *btree.BTree
}

type btreeIterator struct {
	tree  *btree.BTree
	idx   int
	items []btreeItem
}

type btreeUpdater struct {
	bkv  *btreeKV
	tree *btree.BTree
}

type btreeItem struct {
	key []byte
	val []byte
}

func (bi btreeItem) Less(item btree.Item) bool {
	bi2 := item.(btreeItem)
	return bytes.Compare(bi.key, bi2.key) < 0
}

func MakeBTreeKV() (KV, error) {
	return &btreeKV{
		tree: btree.New(16),
	}, nil
}

func iterate(tree *btree.BTree, key []byte) Iterator {
	var items []btreeItem
	tree.AscendGreaterOrEqual(btreeItem{key: key},
		func(item btree.Item) bool {
			items = append(items, item.(btreeItem))
			return len(items) < 8
		})

	return &btreeIterator{
		tree:  tree,
		items: items,
	}
}

func (bkv *btreeKV) Iterate(key []byte) (Iterator, error) {
	bkv.treeMutex.Lock()
	tree := bkv.tree
	bkv.treeMutex.Unlock()

	return iterate(tree, key), nil
}

func (bit *btreeIterator) Item(fn func(key, val []byte) error) error {
	if len(bit.items) == 0 {
		return io.EOF
	}

	if bit.idx == len(bit.items) {
		key := bit.items[len(bit.items)-1].key

		bit.idx = 0
		bit.items = bit.items[:0]

		var rest bool
		bit.tree.AscendGreaterOrEqual(btreeItem{key: key},
			func(item btree.Item) bool {
				if rest {
					bit.items = append(bit.items, item.(btreeItem))
				}
				rest = true
				return len(bit.items) < 8
			})

		if len(bit.items) == 0 {
			return io.EOF
		}
	}

	err := fn(bit.items[bit.idx].key, bit.items[bit.idx].val)
	bit.idx += 1
	return err
}

func (bit *btreeIterator) Close() {
	// Nothing.
}

func (bkv *btreeKV) Update(key []byte, fn func(val []byte) ([]byte, error)) error {
	bkv.updateMutex.Lock()
	defer bkv.updateMutex.Unlock()

	bkv.treeMutex.Lock()
	tree := bkv.tree.Clone()
	bkv.treeMutex.Unlock()

	item := tree.Get(btreeItem{key: key})

	var val []byte
	if item != nil {
		val = item.(btreeItem).val
	}

	val, err := fn(val)
	if err != nil {
		return err
	}

	if len(val) == 0 {
		tree.Delete(btreeItem{key: key})
	} else {
		tree.ReplaceOrInsert(btreeItem{key: key, val: val})
	}

	bkv.treeMutex.Lock()
	bkv.tree = tree
	bkv.treeMutex.Unlock()
	return nil
}

func (bkv *btreeKV) Get(key []byte, fn func(val []byte) error) error {
	bkv.treeMutex.Lock()
	tree := bkv.tree
	bkv.treeMutex.Unlock()

	item := tree.Get(btreeItem{key: key})
	if item == nil {
		return io.EOF
	}
	return fn(item.(btreeItem).val)
}

func (bkv *btreeKV) Updater() (Updater, error) {
	bkv.updateMutex.Lock()

	bkv.treeMutex.Lock()
	tree := bkv.tree.Clone()
	bkv.treeMutex.Unlock()

	return btreeUpdater{
		bkv:  bkv,
		tree: tree,
	}, nil
}

func (bu btreeUpdater) Iterate(key []byte) (Iterator, error) {
	return iterate(bu.tree, key), nil
}

func (bu btreeUpdater) Get(key []byte, fn func(val []byte) error) error {
	item := bu.tree.Get(btreeItem{key: key})
	if item == nil {
		return io.EOF
	}
	return fn(item.(btreeItem).val)
}

func (bu btreeUpdater) Set(key, val []byte) error {
	bu.tree.ReplaceOrInsert(btreeItem{key: key, val: val})
	return nil
}

func (bu btreeUpdater) Commit(sync bool) error {
	bu.bkv.treeMutex.Lock()
	bu.bkv.tree = bu.tree
	bu.bkv.treeMutex.Unlock()

	bu.bkv.updateMutex.Unlock()
	return nil
}

func (bu btreeUpdater) Rollback() {
	bu.bkv.updateMutex.Unlock()
}
