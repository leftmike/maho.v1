package test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/engine/kv"
	"github.com/leftmike/maho/testutil"
)

func makeKey(n int64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(n))
	return key
}

func withReadTx(t *testing.T, db kv.DB, fn func(tx kv.ReadTx) error) {
	t.Helper()

	tx, err := db.ReadTx()
	if err != nil {
		t.Errorf("ReadTx() failed with %s", err)
		return
	}
	defer func() {
		tx.Discard()
	}()

	err = fn(tx)
	if err != nil {
		t.Error(err)
	}
}

func withWriteTx(t *testing.T, db kv.DB, commit bool, fn func(tx kv.WriteTx) error) {
	t.Helper()

	tx, err := db.WriteTx()
	if err != nil {
		t.Errorf("WriteTx() failed with %s", err)
		return
	}
	defer func() {
		if commit {
			err := tx.Commit()
			if err != nil {
				t.Errorf("Commit() failed with %s", err)
			}
		} else {
			tx.Discard()
		}
	}()

	err = fn(tx)
	if err != nil {
		t.Error(err)
	}
}

func readKeyRange(t *testing.T, db kv.DB, first, last, step int64,
	fn func(tx kv.ReadTx, n int64, key []byte) error) {

	t.Helper()

	withReadTx(t, db,
		func(tx kv.ReadTx) error {
			for n := first; n <= last; n += step {
				err := fn(tx, n, makeKey(n))
				if err != nil {
					return err
				}
			}
			return nil
		})
}

func writeKeyRange(t *testing.T, db kv.DB, commit bool, first, last, step int64,
	fn func(tx kv.WriteTx, n int64, key []byte) error) {

	t.Helper()

	withWriteTx(t, db, commit,
		func(tx kv.WriteTx) error {
			for n := first; n <= last; n += step {
				err := fn(tx, n, makeKey(n))
				if err != nil {
					return err
				}
			}
			return nil
		})
}

func setRange(t *testing.T, db kv.DB, commit bool, prefix string, first, last, step int64) {

	t.Helper()

	writeKeyRange(t, db, commit, first, last, step,
		func(tx kv.WriteTx, n int64, key []byte) error {
			err := tx.Set(append([]byte(prefix), key...), []byte(fmt.Sprintf("%d", n)))
			if err != nil {
				return fmt.Errorf("Set(%d) failed with %s", n, err)
			}
			return nil
		})
}

func deleteRange(t *testing.T, db kv.DB, commit bool, prefix string, first, last, step int64) {

	t.Helper()

	writeKeyRange(t, db, commit, first, last, step,
		func(tx kv.WriteTx, n int64, key []byte) error {
			err := tx.Delete(append([]byte(prefix), key...))
			if err != nil {
				return fmt.Errorf("Delete(%d) failed with %s", n, err)
			}
			return nil
		})
}

func deleteAll(t *testing.T, db kv.DB, commit bool, prefix string) {
	t.Helper()

	tx, err := db.WriteTx()
	if err != nil {
		t.Errorf("WriteTx() failed with %s", err)
		return
	}
	defer func() {
		if commit {
			err := tx.Commit()
			if err != nil {
				t.Errorf("Commit() failed with %s", err)
			}
		} else {
			tx.Discard()
		}
	}()

	it := tx.Iterate([]byte(prefix))
	defer it.Close()

	it.Rewind()
	for it.Valid() {
		key := it.KeyCopy()
		err := tx.Delete(key)
		if err != nil {
			t.Errorf("Delete() failed with %s", err)
			return
		}
		it.Next()
	}
}

func getRange(t *testing.T, db kv.DB, prefix string, first, last, step int64) {
	t.Helper()

	readKeyRange(t, db, first, last, step,
		func(tx kv.ReadTx, n int64, key []byte) error {
			err := tx.Get(append([]byte(prefix), key...),
				func(val []byte) error {
					want := fmt.Sprintf("%d", n)
					if string(val) != want {
						return fmt.Errorf("got %s want %s", string(val), want)
					}
					return nil
				})
			if err != nil {
				return fmt.Errorf("Get(%d) failed with %s", n, err)
			}
			return nil
		})
}

func getValueRange(t *testing.T, db kv.DB, prefix string, first, last, step int64) {
	t.Helper()

	readKeyRange(t, db, first, last, step,
		func(tx kv.ReadTx, n int64, key []byte) error {
			val, err := tx.GetValue(append([]byte(prefix), key...))
			want := fmt.Sprintf("%d", n)
			if string(val) != want {
				return fmt.Errorf("GetValue(%d) got %s want %s", n, string(val), want)
			}
			if err != nil {
				return fmt.Errorf("GetValue(%d) failed with %s", n, err)
			}
			return nil
		})
}

func notFoundRange(t *testing.T, db kv.DB, prefix string, first, last, step int64) {
	t.Helper()

	readKeyRange(t, db, first, last, step,
		func(tx kv.ReadTx, n int64, key []byte) error {
			err := tx.Get(append([]byte(prefix), key...),
				func(val []byte) error {
					return errors.New("key found")
				})
			if err == kv.ErrKeyNotFound {
				return nil
			}
			return fmt.Errorf("Get(%d) did not fail with ErrKeyNotFound", n)
		})
}

func iterateRange(t *testing.T, db kv.DB, prefix string, first, last, step int64,
	fn func(it kv.Iterator)) {

	t.Helper()

	tx, err := db.ReadTx()
	if err != nil {
		t.Errorf("ReadTx() failed with %s", err)
		return
	}
	defer func() {
		tx.Discard()
	}()

	cnt := 0
	for n := first; n <= last; n += step {
		cnt += 1
	}

	it := tx.Iterate([]byte(prefix))
	defer it.Close()

	fn(it)

	n := first
	for it.Valid() {
		wantKey := make([]byte, 8)
		binary.BigEndian.PutUint64(wantKey, uint64(n))
		wantKey = append([]byte(prefix), wantKey...)
		if !bytes.Equal(it.Key(), wantKey) {
			t.Errorf("Key() got %v want %v", it.Key(), wantKey)
		}
		err = it.Value(
			func(val []byte) error {
				wantVal := fmt.Sprintf("%d", n)
				if string(val) != wantVal {
					return fmt.Errorf("got %s want %s", string(val), wantVal)
				}
				return nil
			})
		if err != nil {
			t.Errorf("Value() failed with %s", err)
		}

		n += step
		cnt -= 1
		it.Next()
	}
	if cnt > 0 {
		t.Errorf("expected %d more keys", cnt)
	} else if cnt < 0 {
		t.Errorf("expected %d less keys", -cnt)
	}
}

func seekRange(t *testing.T, db kv.DB, prefix string, first, last, step int64) {
	t.Helper()

	iterateRange(t, db, prefix, first, last, step,
		func(it kv.Iterator) {
			it.Seek(append([]byte(prefix), makeKey(first)...))
		})
}

func rewindRange(t *testing.T, db kv.DB, prefix string, first, last, step int64) {
	t.Helper()

	iterateRange(t, db, prefix, first, last, step,
		func(it kv.Iterator) {
			it.Rewind()
		})
}

func notFoundGet(t *testing.T, db kv.DB, key string) {
	t.Helper()

	withReadTx(t, db,
		func(tx kv.ReadTx) error {
			err := tx.Get([]byte(key),
				func(val []byte) error {
					return nil
				})
			if err == kv.ErrKeyNotFound {
				return nil
			}
			return errors.New("Get() did not fail with ErrKeyNotFound")
		})
}

func withDB(t *testing.T, e kv.Engine, path string, fn func(t *testing.T, db kv.DB)) {
	t.Helper()

	db, err := e.Open(path)
	if err != nil {
		t.Fatalf("Open(%s) failed with %s", path, err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatalf("Close(%s) failed with %s", path, err)
		}
	}()

	fn(t, db)
}

func runTest1(t *testing.T, db kv.DB) {
	t.Helper()

	notFoundGet(t, db, "this that")

	notFoundRange(t, db, "test1-one", 0, 100, 1)
	setRange(t, db, true, "test1-one", 0, 100, 1)
	notFoundGet(t, db, "test1-one-key")

	getRange(t, db, "test1-one", 0, 100, 1)
	getValueRange(t, db, "test1-one", 0, 100, 1)
	seekRange(t, db, "test1-one", 0, 100, 1)
	rewindRange(t, db, "test1-one", 0, 100, 1)
	seekRange(t, db, "test1-one", 20, 100, 1)
	deleteRange(t, db, true, "test1-one", 0, 100, 2)
	seekRange(t, db, "test1-one", 1, 99, 2)
	getRange(t, db, "test1-one", 1, 99, 2)
	notFoundRange(t, db, "test1-one", 0, 100, 2)

	setRange(t, db, false, "test1-one", 200, 300, 1)
	notFoundRange(t, db, "test1-one", 200, 300, 1)

	setRange(t, db, true, "test1-two", 1, 1000, 1)
	getRange(t, db, "test1-two", 1, 1000, 1)
	deleteAll(t, db, true, "test1-two")
	notFoundRange(t, db, "test1-two", 1, 1000, 1)
	getRange(t, db, "test1-one", 1, 99, 2)

	deleteAll(t, db, false, "test1-one")
	getRange(t, db, "test1-one", 1, 99, 2)
}

func runTest2(t *testing.T, db kv.DB) {
	t.Helper()

	getRange(t, db, "test1-one", 1, 99, 2)
	getValueRange(t, db, "test1-one", 1, 99, 2)
	notFoundRange(t, db, "test1-one", 0, 100, 2)
	notFoundRange(t, db, "test1-two", 1, 1000, 1)
	seekRange(t, db, "test1-one", 10000, 9999, 1) // no keys found
}

func RunKVTest(t *testing.T, e kv.Engine) {
	t.Helper()

	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

	path := filepath.Join("testdata", "testkv")
	withDB(t, e, path, runTest1)
	withDB(t, e, path, runTest2)
}