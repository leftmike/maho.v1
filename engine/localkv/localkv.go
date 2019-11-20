package localkv

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/engine/kvrows"
)

type localKV struct {
	st Store
}

func NewStore(st Store) kvrows.Store {
	return localKV{st}
}

func (lkv localKV) ReadValue(ctx context.Context, mid uint64, key kvrows.Key) (uint64, []byte,
	error) {

	tx, err := lkv.st.Begin(false)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return 0, nil, err
	}

	w := m.Walk(key.Key)
	defer w.Close()

	kbuf, ok := w.Rewind()
	if !ok {
		return 0, nil, kvrows.ErrKeyNotFound
	}

	k, ok := kvrows.ParseKey(kbuf)
	if !ok {
		return 0, nil, fmt.Errorf("localkv: unable to parse key %v", kbuf)
	}

	var retval []byte
	err = w.Value(
		func(val []byte) error {
			retval = append(make([]byte, 0, len(val)), val...)
			return nil
		})
	if err != nil {
		return 0, nil, err
	}

	return k.Version, retval, nil
}

func (lkv localKV) ListValues(ctx context.Context, mid uint64) ([]kvrows.Key, [][]byte, error) {
	tx, err := lkv.st.Begin(false)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return nil, nil, err
	}

	w := m.Walk(nil)
	defer w.Close()

	kbuf, ok := w.Rewind()

	var keys []kvrows.Key
	var vals [][]byte
	for ok {
		var k kvrows.Key
		k, ok = kvrows.ParseKey(kbuf)
		if !ok {
			return nil, nil, fmt.Errorf("localkv: unable to parse key %v", kbuf)
		}
		keys = append(keys, k)

		err = w.Value(
			func(val []byte) error {
				vals = append(vals, append(make([]byte, 0, len(val)), val...))
				return nil
			})
		if err != nil {
			return nil, nil, err
		}

		kbuf, ok = w.Next()
	}

	return keys, vals, nil
}

func (lkv localKV) WriteValue(ctx context.Context, mid uint64, key kvrows.Key, ver uint64,
	val []byte) error {

	tx, err := lkv.st.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return err
	}

	w := m.Walk(key.Key)
	defer w.Close()

	buf, ok := w.Rewind()
	if !ok {
		if key.Version > 0 {
			return kvrows.ErrKeyNotFound
		}
	} else {
		k, ok := kvrows.ParseKey(buf)
		if !ok {
			return fmt.Errorf("localkv: unable to parse key %v", buf)
		}
		if k.Version != key.Version || ver <= k.Version {
			return kvrows.ErrValueVersionMismatch
		}

		err = w.Delete()
		if err != nil {
			return err
		}
	}

	k := kvrows.Key{
		Key:     key.Key,
		Version: ver,
		Type:    key.Type,
	}
	err = m.Set(k.Encode(), val)
	if err != nil {
		return err
	}

	w.Close()
	return tx.Commit()
}

func (lkv localKV) ScanRelation(ctx context.Context, rel kvrows.Relation, prefix []byte,
	next interface{}) ([]kvrows.Key, [][]byte, interface{}, error) {

	return nil, nil, nil, nil
}

func (lkv localKV) DeleteRelation(ctx context.Context, rel kvrows.Relation,
	keys []kvrows.Key) error {

	return nil
}

func (lkv localKV) UpdateRelation(ctx context.Context, rel kvrows.Relation, keys []kvrows.Key,
	vals []byte) error {

	return nil
}

func (lkv localKV) InsertRelation(ctx context.Context, rel kvrows.Relation, keys []kvrows.Key,
	vals []byte) error {

	return nil
}
