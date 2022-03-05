package kvrows

import (
	"bytes"
	"io"
	"os"
	"sync"

	"github.com/cockroachdb/pebble"
	log "github.com/sirupsen/logrus"
)

type pebbleKV struct {
	mutex sync.Mutex
	db    *pebble.DB
}

type pebbleIterator struct {
	snap   *pebble.Snapshot
	it     *pebble.Iterator
	maxKey []byte
}

type pebbleUpdater struct {
	kv    *pebbleKV
	batch *pebble.Batch
}

func MakePebbleKV(dataDir string, logger *log.Logger) (KV, error) {
	os.MkdirAll(dataDir, 0755)

	db, err := pebble.Open(dataDir, &pebble.Options{Logger: logger})
	if err != nil {
		return nil, err
	}
	return &pebbleKV{
		db: db,
	}, nil
}

func (pkv *pebbleKV) Iterate(minKey, maxKey []byte) (Iterator, error) {
	snap := pkv.db.NewSnapshot()
	it := snap.NewIter(nil)
	it.SeekGE(minKey)

	return pebbleIterator{
		snap:   snap,
		it:     it,
		maxKey: maxKey,
	}, nil
}

func (pit pebbleIterator) Item(fn func(key, val []byte) error) error {
	if !pit.it.Valid() {
		return io.EOF
	}

	key := pit.it.Key()
	if bytes.Compare(pit.maxKey, key) < 0 {
		return io.EOF
	}

	err := fn(key, pit.it.Value())
	if err != nil {
		return err
	}

	pit.it.Next()
	return nil
}

func (pit pebbleIterator) Close() {
	pit.it.Close()
	if pit.snap != nil {
		pit.snap.Close()
	}
}

func (pkv *pebbleKV) Update(key []byte, fn func(val []byte) ([]byte, error)) error {
	pkv.mutex.Lock()
	defer pkv.mutex.Unlock()

	val, closer, err := pkv.db.Get(key)

	var newVal []byte
	if err == pebble.ErrNotFound {
		newVal, err = fn(nil)
	} else if err != nil {
		return err
	} else {
		newVal, err = fn(val)
		closer.Close()
	}

	if err != nil {
		return err
	}

	if len(newVal) == 0 {
		return pkv.db.Delete(key, nil)
	}
	return pkv.db.Set(key, newVal, nil)
}

func (pkv *pebbleKV) Get(key []byte, fn func(val []byte) error) error {
	val, closer, err := pkv.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return io.EOF
		}
		return err
	}
	defer closer.Close()

	return fn(val)
}

func (pkv *pebbleKV) Updater() (Updater, error) {
	pkv.mutex.Lock()

	return pebbleUpdater{
		kv:    pkv,
		batch: pkv.db.NewIndexedBatch(),
	}, nil
}

func (pu pebbleUpdater) Get(key []byte, fn func(val []byte) error) error {
	val, closer, err := pu.batch.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return io.EOF
		}
		return err
	}
	defer closer.Close()

	return fn(val)
}

func (pu pebbleUpdater) Set(key, val []byte) error {
	return pu.batch.Set(key, val, nil)
}

func (pu pebbleUpdater) Commit(sync bool) error {
	opt := pebble.NoSync
	if sync {
		opt = pebble.Sync
	}
	err := pu.batch.Commit(opt)
	pu.kv.mutex.Unlock()
	return err
}

func (pu pebbleUpdater) Rollback() {
	pu.batch.Close()
	pu.kv.mutex.Unlock()
}
