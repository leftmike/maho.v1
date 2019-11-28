package localkv

import (
	"bytes"
	"context"
	"fmt"
	"io"

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

func nextKey(rel kvrows.Relation, w Walker, k kvrows.Key, maxVer uint64,
	fkv func(key kvrows.Key, val []byte) error) (kvrows.Key, error) {

	for {
		if k.Type == kvrows.ProposalKeyType {
			found := false
			err := w.Value(
				func(val []byte) error {
					if len(val) == 0 || val[0] != kvrows.ProposalValue {
						return fmt.Errorf("localkv: unable to parse value for key %v: %v", k, val)
					}
					txk, pval, ok := kvrows.ParseProposalValue(val)
					if !ok {
						return fmt.Errorf("localkv: unable to parse value for key %v: %v", k, val)
					}

					if txk.Equal(rel.TxKey()) {
						if k.Version < rel.CurrentStatement() {
							found = true
						}
					} else {
						st := rel.GetTransactionState(txk)
						if st == kvrows.CommittedState {
							found = true
						} else if st != kvrows.AbortedState {
							// Can't ignore this proposal.
							return &kvrows.ErrBlockingProposals{
								TxKeys: []kvrows.TransactionKey{txk.Copy()},
								Keys:   []kvrows.Key{k.Copy()},
							}
						}
					}

					if found {
						return fkv(k, pval)
					}
					return nil
				})
			if found || err != nil {
				return k, err
			}
		} else if k.Type == kvrows.DurableKeyType && k.Version <= maxVer {
			return k, w.Value(
				func(val []byte) error {
					return fkv(k, val)
				})
		}

		kbuf, ok := w.Next()
		if !ok {
			break
		}
		k, ok = kvrows.ParseKey(kbuf)
		if !ok {
			return kvrows.Key{}, fmt.Errorf("localkv: unable to parse key %v", kbuf)
		}
	}
	return k, io.EOF
}

func (lkv localKV) ScanRelation(ctx context.Context, rel kvrows.Relation, maxVer uint64,
	prefix []byte, num int, seek []byte) ([]kvrows.Key, [][]byte, []byte, error) {

	tx, err := lkv.st.Begin(false)
	if err != nil {
		return nil, nil, nil, err
	}
	defer tx.Rollback()

	m, err := tx.Map(rel.MapID())
	if err != nil {
		return nil, nil, nil, err
	}

	w := m.Walk(prefix)
	defer w.Close()

	var kbuf []byte
	var ok bool
	if seek == nil {
		kbuf, ok = w.Rewind()
	} else {
		kbuf, ok = w.Seek(seek)
	}
	if !ok {
		return nil, nil, nil, io.EOF
	}
	k, ok := kvrows.ParseKey(kbuf)
	if !ok {
		return nil, nil, nil, fmt.Errorf("localkv: unable to parse key %v", kbuf)
	}

	if num < 1 {
		num = 1
	} else if num > 4096 {
		num = 4096
	}

	var keys []kvrows.Key
	var vals [][]byte
	for len(keys) < num {
		// Scan and find the greatest version of a key or a proposal key.
		k, err = nextKey(rel, w, k, maxVer,
			func(key kvrows.Key, val []byte) error {
				if len(val) == 0 || val[0] == kvrows.TombstoneValue {
					return nil
				}
				keys = append(keys, key.Copy())
				// XXX: parse the val here rather than just copying it to be parsed later
				vals = append(vals, append(make([]byte, 0, len(val)), val...))
				return nil
			})
		if err != nil {
			// XXX: only return keys, vals, and k if err is ErrBlockingProposals
			return keys, vals, k.Copy().Key, err
		}

		// Skip along to the next key.
		for {
			kbuf, ok = w.Next()
			if !ok {
				return keys, vals, nil, io.EOF
			}
			nxt, ok := kvrows.ParseKey(kbuf)
			if !ok {
				return nil, nil, nil, fmt.Errorf("localkv: unable to parse key %v", kbuf)
			}
			if !bytes.Equal(k.Key, nxt.Key) {
				k = nxt
				break
			}
		}
	}

	return keys, vals, k.Copy().Key, nil
}

/*
func (lkv localKV) seekKey(rel kvrows.Relation, w Walker) (kvrows.Key, error) {
	kbuf, ok := w.Rewind()
	if !ok {
		return kvrows.Key{}, io.EOF
	}
	k, ok := kvrows.ParseKey(kbuf)
	if !ok {
		return kvrows.Key{}, fmt.Errorf("localkv: unable to parse key %v", kbuf)
	}

	for {
		if k.Type == kvrows.ProposalKeyType {
			blockingProposal := false
			err = w.Value(
				func(val []byte) error {
					if len(val) == 0 || val[0] != kvrows.ProposalValue {
						return fmt.Errorf("localkv: unable to parse value for key %v: %v",
							kbuf, val)
					}
					txk, pval, ok := kvrows.ParseProposalValue(val)
					if !ok {
						return fmt.Errorf("localkv: unable to parse value for key %v: %v",
							kbuf, val)
					}

					if txk.Equal(rel.TxKey()) {
						if k.Version < rel.CurrentStatement() {
							found = true
						}
					} else {
						st := rel.GetTransactionState(txk)
						if st == kvrows.CommittedState {
							found = true
						} else if st != kvrows.AbortedState {
							// Can't ignore this proposal.
							blockingProposal = true
							return &kvrows.ErrBlockingProposals{
								TxKeys: []kvrows.TransactionKey{txk},
								Keys:   []kvrows.Key{k},
							}
						}
					}

					if found {
						if pval[0] != kvrows.TombstoneValue {
							k = k.Copy()
							keys = append(keys, k)
							vals = append(vals,
								append(make([]byte, 0, len(pval)), pval...))
						}
					}
					return nil
				})
			if err != nil {
				if blockingProposal {
					return keys, vals, k.Key, err
				}
				return nil, nil, nil, err
			}
		} else if k.Type == kvrows.DurableKeyType && k.Version <= maxVer {
			err = w.Value(
				func(val []byte) error {
					if len(val) == 0 || val[0] == kvrows.TombstoneValue {
						return nil
					}
					k = k.Copy()
					keys = append(keys, k)
					// XXX: parse the val here rather than just copying it to be parsed later
					vals = append(vals, append(make([]byte, 0, len(val)), val...))
					return nil
				})
			if err != nil {
				return nil, nil, nil, err
			}
			found = true
		}

		if found {
			break
		}

		kbuf, ok = w.Next()
		if !ok {
			return keys, vals, nil, io.EOF
		}
		k, ok = kvrows.ParseKey(kbuf)
		if !ok {
			return nil, nil, nil, fmt.Errorf("localkv: unable to parse key %v", kbuf)
		}
	}

	return k, nil
}
*/

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
	/*
		tx, err := lkv.st.Begin(true)
		if err != nil {
			return nil, nil, nil, err
		}
		defer tx.Rollback()

		m, err := tx.Map(rel.MapID())
		if err != nil {
			return nil, nil, nil, err
		}

		for kdx, key := range keys {
			// should be a func
			w := m.Walk(key.Key)
			defer w.Close()

			k, err := lkv.seekKey(w)

		}
	*/
	return nil
}
