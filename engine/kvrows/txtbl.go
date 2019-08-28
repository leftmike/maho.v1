package kvrows

import (
	"errors"
	"fmt"
	"io"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

var (
	existingValue = errors.New("kvrows: existing value for key")
)

type getTransaction func(tid uint32) (TransactionState, uint64, error)

type transactedTable struct {
	st      Store
	getTx   getTransaction
	ver     uint64
	mid     uint64
	tid     uint32
	colKeys []engine.ColumnKey
	// XXX: need to keep track of which keys were set / modified
	// modifiedKeys [][]byte // prefixes?
	// and a routine to return the list of modified keys
}

type transactedRows struct {
	txtbl    *transactedTable
	keysOnly bool
}

func MakeTransactedTable(st Store, getTx getTransaction, ver, mid uint64, tid uint32,
	colKeys []engine.ColumnKey) *transactedTable {

	return &transactedTable{
		st:      st,
		getTx:   getTx,
		ver:     ver,
		mid:     mid,
		tid:     tid,
		colKeys: colKeys,
	}
}

func (txtbl *transactedTable) existingKey(m Mapper, key []byte) (bool, error) {
	w := m.Walk(KeyPrefix(key))
	defer w.Close()

	k, ok := w.Rewind()
	if !ok {
		return false, nil
	}
	for {
		switch GetKeyType(k) {
		case ProposalKeyType:
			tid, _, ok := ParseProposalKey(k, txtbl.colKeys, nil)
			if !ok {
				return false, fmt.Errorf("kvrows: bad proposal key: %v", k)
			}
			if tid == txtbl.tid {
				// XXX: need to check the value to see if it is a tombstone
				return true, nil
			}

			state, ver, err := txtbl.getTx(tid)
			if err != nil {
				return false, err
			}

			if state == ActiveTransaction {
				return false, fmt.Errorf("kvrows: conflicting change to %v", key)
			} else if state == CommittedTransaction {
				if ver > txtbl.ver {
					return false, fmt.Errorf("kvrows: conflicting change to %v", key)
				}
				// XXX: need to check the value to see if it is a tombstone
				return true, nil
			}

			// Aborted transaction: ignore this key and continue.

		case DurableKeyType:

			// XXX: check the version of the durable key and then check the value to see if
			// it is a tombstone

		default:
			panic("expected proposal key or durable key")
		}

		k, ok = w.Next()
		if !ok {
			break
		}
	}

	return false, nil
}

func (txtbl *transactedTable) Insert(sid uint32, row []sql.Value) error {
	tx, err := txtbl.st.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(txtbl.mid)
	if err != nil {
		return err
	}

	key := MakeProposalKey(row, txtbl.colKeys, txtbl.tid, sid)
	found, err := txtbl.existingKey(m, key)
	if err != nil {
		return err
	} else if found {
		return existingValue
	}

	err = m.Set(key, MakeRowValue(row))
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	// XXX: save the key and/or add it to the transaction state

	return nil
}

func (txtbl *transactedTable) Delete(sid uint32, row []sql.Value) error {
	return notImplemented
}

func (txtbl *transactedTable) Update(sid uint32, row []sql.Value) error {
	return notImplemented
}

func (txtbl *transactedTable) Rows(keysOnly bool) *transactedRows {
	return &transactedRows{
		txtbl:    txtbl,
		keysOnly: keysOnly,
	}
}

func (txrows *transactedRows) Next(dest []sql.Value) error {
	return io.EOF
}
