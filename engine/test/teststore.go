package test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/localkv"
)

type row struct {
	key   string
	value uint64
}

func insertRows(t *testing.T, tx localkv.Tx, mid uint64, layer byte, rows []row) {
	t.Helper()

	m, err := tx.Map(mid, layer)
	if err != nil {
		t.Errorf("Map(%d) failed with %s", mid, err)
		return
	}

	for _, i := range rand.Perm(len(rows)) {
		err = m.Set([]byte(rows[i].key), kvrows.EncodeUint64(rows[i].value))
		if err != nil {
			t.Errorf("Set(%d) failed with %s", mid, err)
			break
		}
	}
}

func deleteRows(t *testing.T, tx localkv.Tx, mid uint64, layer byte, rows map[string]struct{}) {
	t.Helper()

	m, err := tx.Map(mid, layer)
	if err != nil {
		t.Errorf("Map(%d) failed with %s", mid, err)
		return
	}

	w := m.Walk(nil)
	defer w.Close()

	key, ok := w.Rewind()
	for ok {
		if _, found := rows[string(key)]; found {
			err = w.Delete()
			if err != nil {
				t.Error(err)
			}
		}
		key, ok = w.Next()
	}
}

func updateRows(t *testing.T, tx localkv.Tx, mid uint64, layer byte, rows map[string]uint64) {
	t.Helper()

	m, err := tx.Map(mid, layer)
	if err != nil {
		t.Errorf("Map(%d) failed with %s", mid, err)
		return
	}

	w := m.Walk(nil)
	defer w.Close()

	key, ok := w.Rewind()
	for ok {
		if u, found := rows[string(key)]; found {
			err = m.Set(key, kvrows.EncodeUint64(u))
			if err != nil {
				t.Error(err)
			}
		}
		key, ok = w.Next()
	}
}

func selectRows(t *testing.T, tx localkv.Tx, mid uint64, layer byte, seek string, rows []row) {
	t.Helper()

	m, err := tx.Map(mid, layer)
	if err != nil {
		t.Errorf("Map(%d) failed with %s", mid, err)
		return
	}

	w := m.Walk(nil)
	defer w.Close()

	var key []byte
	var ok bool
	if seek == "" {
		key, ok = w.Rewind()
	} else {
		key, ok = w.Seek([]byte(seek))
	}
	for i := 0; ok; i += 1 {
		if i >= len(rows) {
			t.Fatalf("Walk(%d) too many rows", i)
		}
		if string(key) != rows[i].key {
			t.Errorf("Walk(%d) got key %s want key %s", i, string(key), rows[i].key)
		}
		err = w.Value(
			func(val []byte) error {
				if len(val) != 8 {
					return fmt.Errorf("len(%v) != 8", val)
				}
				u := kvrows.DecodeUint64(val)
				if u != rows[i].value {
					return fmt.Errorf("Value(%v) got %d want %d", val, u, rows[i].value)
				}
				return nil
			})
		if err != nil {
			t.Error(err)
		}
		key, ok = w.Next()
	}
}

func testGetSet(t *testing.T, tx localkv.Tx, mid uint64, layer byte) {
	m, err := tx.Map(mid, layer)
	if err != nil {
		t.Errorf("Map(%d) failed with %s", mid, err)
		return
	}

	cases := []struct {
		key string
		val []byte
	}{
		{"abcd", []byte{9, 8, 7, 6, 5, 4}},
	}

	for _, c := range cases {
		vf := func(val []byte) error {
			if bytes.Compare(val, c.val) != 0 {
				t.Errorf("Get(%s): got %v want %v", c.key, val, c.val)
			}
			return nil
		}

		err = m.Get([]byte(c.key), vf)
		if err == nil {
			t.Errorf("Get(%s) did not fail", c.key)
		}
		err = m.Set([]byte(c.key), c.val)
		if err != nil {
			t.Errorf("Set(%s) failed with %s", c.key, err)
		}
		err = m.Get([]byte(c.key), vf)
		if err != nil {
			t.Errorf("Get(%s) failed with %s", c.key, err)
		}
	}
}

func withCommit(t *testing.T, st localkv.Store, tf func(t *testing.T, tx localkv.Tx)) {
	t.Helper()

	tx, err := st.Begin(true)
	if err != nil {
		t.Errorf("Begin() failed with %s", err)
		return
	}
	tf(t, tx)
	err = tx.Commit()
	if err != nil {
		t.Errorf("Commit() failed with %s", err)
	}

	// Rollback should be a no-op after Commit
	err = tx.Rollback()
	if err != nil {
		t.Errorf("Rollback() failed with %s", err)
	}
}

func withRollback(t *testing.T, st localkv.Store, writable bool,
	tf func(t *testing.T, tx localkv.Tx)) {

	t.Helper()

	tx, err := st.Begin(writable)
	if err != nil {
		t.Errorf("Begin() failed with %s", err)
		return
	}
	tf(t, tx)
	err = tx.Rollback()
	if err != nil {
		t.Errorf("Rollback() failed with %s", err)
	}
}

func runLocalKVTest(t *testing.T, st localkv.Store, mid uint64, layer byte) {
	withCommit(t, st, func(t *testing.T, tx localkv.Tx) {})
	withRollback(t, st, true, func(t *testing.T, tx localkv.Tx) {})
	withRollback(t, st, false, func(t *testing.T, tx localkv.Tx) {})

	rows1 := []row{
		{"ABC", 1},
		{"a", 2},
		{"ab", 3},
		{"abc", 4},
		{"xyz", 5},
	}

	rows2 := []row{
		{"ABC", 10},
		{"a", 20},
		{"ab", 30},
		{"abc", 40},
		{"xyz", 50},
	}
	withCommit(t, st,
		func(t *testing.T, tx localkv.Tx) {
			insertRows(t, tx, mid, layer, rows1)
			selectRows(t, tx, mid, layer, "", rows1)
		})
	withRollback(t, st, true,
		func(t *testing.T, tx localkv.Tx) {
			insertRows(t, tx, mid, layer, rows2)
			selectRows(t, tx, mid, layer, "", rows2)
		})
	withCommit(t, st,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid, layer, "", rows1)
		})
	withRollback(t, st, false,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid, layer, "", rows1)
		})

	rows3 := []row{
		{"ABC", 1},
		{"a", 200},
		{"ab", 3},
		{"abc", 400},
		{"xyz", 5},
	}
	update1 := map[string]uint64{
		"a":   200,
		"abc": 400,
	}
	withRollback(t, st, true,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid, layer, "", rows1)
			updateRows(t, tx, mid, layer, update1)
			selectRows(t, tx, mid, layer, "", rows3)
		})
	withCommit(t, st,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid, layer, "", rows1)
			updateRows(t, tx, mid, layer, update1)
		})
	withCommit(t, st,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid, layer, "", rows3)
		})
	withRollback(t, st, false,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid, layer, "", rows3)
		})

	rows4 := []row{
		{"a", 200},
		{"ab", 3},
		{"abc", 400},
	}
	delete1 := map[string]struct{}{
		"ABC": struct{}{},
		"xyz": struct{}{},
	}
	withRollback(t, st, true,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid, layer, "", rows3)
			deleteRows(t, tx, mid, layer, delete1)
			selectRows(t, tx, mid, layer, "", rows4)
		})
	withCommit(t, st,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid, layer, "", rows3)
			deleteRows(t, tx, mid, layer, delete1)
		})
	withCommit(t, st,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid, layer, "", rows4)
		})
	withRollback(t, st, false,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid, layer, "", rows4)
		})

	withCommit(t, st,
		func(t *testing.T, tx localkv.Tx) {
			insertRows(t, tx, mid+10, layer, rows1)
		})
	withCommit(t, st,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid+10, layer, "", rows1)
			selectRows(t, tx, mid, layer, "", rows4)
		})
	withRollback(t, st, false,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid+10, layer, "", rows1)
			selectRows(t, tx, mid, layer, "", rows4)
		})

	rows5 := []row{
		{"a", 1},
		{"ab", 2},
		{"abc", 3},
		{"def", 4},
		{"ghijkl", 5},
		{"m", 6},
		{"no", 7},
		{"nopq", 8},
		{"xyz", 9},
	}
	withCommit(t, st,
		func(t *testing.T, tx localkv.Tx) {
			insertRows(t, tx, mid+20, layer, rows5)
		})
	withCommit(t, st,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid+20, layer, "", rows5)
			selectRows(t, tx, mid+20, layer, "ABC", rows5)
			selectRows(t, tx, mid+20, layer, "a", rows5)
			selectRows(t, tx, mid+20, layer, "ab", rows5[1:])
			selectRows(t, tx, mid+20, layer, "bcd", rows5[3:])
			selectRows(t, tx, mid+20, layer, "z", nil)
		})
	withRollback(t, st, false,
		func(t *testing.T, tx localkv.Tx) {
			selectRows(t, tx, mid+20, layer, "", rows5)
			selectRows(t, tx, mid+20, layer, "ABC", rows5)
			selectRows(t, tx, mid+20, layer, "a", rows5)
			selectRows(t, tx, mid+20, layer, "ab", rows5[1:])
			selectRows(t, tx, mid+20, layer, "bcd", rows5[3:])
			selectRows(t, tx, mid+20, layer, "z", nil)
		})

	withCommit(t, st,
		func(t *testing.T, tx localkv.Tx) {
			testGetSet(t, tx, mid+30, layer)
		})

	withRollback(t, st, true,
		func(t *testing.T, tx localkv.Tx) {
			testGetSet(t, tx, mid+40, layer)
		})
}

func RunLocalKVTest(t *testing.T, st localkv.Store) {
	runLocalKVTest(t, st, 1000, 0)
	runLocalKVTest(t, st, 1000, 1)
	runLocalKVTest(t, st, 2000, 1)
	runLocalKVTest(t, st, 2000, 123)
}
