package kvrows

import (
	"fmt"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

var (
	versionedPrimary = []engine.ColumnKey{engine.MakeColumnKey(0, false)}
)

type versionedTable struct {
	st  Store
	mid uint64
}

func MakeVersionedTable(st Store, mid uint64) *versionedTable {
	return &versionedTable{
		st:  st,
		mid: mid,
	}
}

// Get the value of a key; return the value version or an error.
func (vtbl *versionedTable) Get(key sql.Value, vf func(val []byte) error) (uint64, error) {
	tx, err := vtbl.st.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	m, err := tx.Map(vtbl.mid)
	if err != nil {
		return 0, err
	}

	w := m.Walk(MakePrefix([]sql.Value{key}, versionedPrimary))
	defer w.Close()
	k, ok := w.Rewind()
	if !ok {
		return 0, ErrKeyNotFound
	}

	ver, ok := ParseDurableKey(k, versionedPrimary, []sql.Value{nil})
	if !ok {
		return 0, fmt.Errorf("kvrows: unable to parse key %v", k)
	}

	err = w.Value(vf)
	if err != nil {
		return 0, err
	}

	if _, ok = w.Next(); ok {
		return 0, fmt.Errorf("kvrows: versioned table %d: multiple rows with identical key: %s",
			vtbl.mid, key)
	}
	return ver, nil
}

// Conditionally set a value: if the key does not exist, ver must be 0; otherwise, ver
// must equal the existing value.
func (vtbl *versionedTable) Set(key sql.Value, ver uint64, value []byte) error {
	tx, err := vtbl.st.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(vtbl.mid)
	if err != nil {
		return err
	}

	w := m.Walk(MakePrefix([]sql.Value{key}, versionedPrimary))
	defer w.Close()
	k, ok := w.Rewind()
	if !ok {
		if ver != 0 {
			return ErrValueVersionMismatch
		}
	} else {
		curVer, ok := ParseDurableKey(k, versionedPrimary, []sql.Value{nil})
		if !ok {
			return fmt.Errorf("kvrows: unable to parse key %v", k)
		}
		if ver != curVer {
			return ErrValueVersionMismatch
		}

		err = w.Delete()
		if err != nil {
			return err
		}
	}

	err = m.Set(MakeDurableKey([]sql.Value{key}, versionedPrimary, ver+1), value)
	if err != nil {
		return err
	}

	w.Close()
	return tx.Commit()
}

// List all keys and values.
func (vtbl *versionedTable) List(kvf func(key sql.Value, ver uint64, val []byte) error) error {
	tx, err := vtbl.st.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(vtbl.mid)
	if err != nil {
		return err
	}

	w := m.Walk(nil)
	defer w.Close()
	k, ok := w.Rewind()
	for ok {
		key := []sql.Value{nil}
		ver, parsed := ParseDurableKey(k, versionedPrimary, key)
		if !parsed {
			return fmt.Errorf("kvrows: unable to parse key %v", k)
		}

		err = w.Value(
			func(val []byte) error {
				return kvf(key[0], ver, val)
			})
		if err != nil {
			return err
		}

		k, ok = w.Next()
	}

	return nil
}
