package localkv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/leftmike/maho/engine/kvrows"
)

type localKV struct {
	st Store
}

func NewStore(st Store) kvrows.Store {
	return localKV{
		st: st,
	}
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

	w := m.Walk(key.SQLKey)
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

	w := m.Walk(key.SQLKey)
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
		SQLKey:  key.SQLKey,
		Version: ver,
	}
	err = m.Set(k.Encode(), val)
	if err != nil {
		return err
	}

	w.Close()
	return tx.Commit()
}

func appendKeysVals(keys []kvrows.Key, vals [][]byte, k kvrows.Key, val []byte) ([]kvrows.Key,
	[][]byte) {

	if len(val) > 0 && val[0] != kvrows.TombstoneValue {
		// XXX: parse the val here rather than just copying it to be parsed later
		return append(keys, k.Copy()),
			append(vals, append(make([]byte, 0, len(val)), val...))
	}
	return keys, vals
}

func (lkv localKV) ScanRelation(ctx context.Context, getState kvrows.GetTxState,
	tid kvrows.TransactionID, sid, mid, maxVer uint64, num int, seek []byte) ([]kvrows.Key,
	[][]byte, []byte, error) {

	tx, err := lkv.st.Begin(false)
	if err != nil {
		return nil, nil, nil, err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return nil, nil, nil, err
	}

	w := m.Walk(nil)
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
		for {
			if k.Version == kvrows.ProposalVersion {
				found := false
				blockingProposal := false
				err := w.Value(
					func(val []byte) error {
						if len(val) == 0 || val[0] != kvrows.ProposalValue {
							return fmt.Errorf("localkv: unable to parse value for key %v: %v", k,
								val)
						}
						proTID, proposals, ok := kvrows.ParseProposalValue(val)
						if !ok {
							return fmt.Errorf("localkv: unable to parse value for key %v: %v", k,
								val)
						}

						if proTID == tid {
							for pdx := len(proposals) - 1; pdx >= 0; pdx -= 1 {
								if proposals[pdx].SID < sid {
									keys, vals = appendKeysVals(keys, vals, k,
										proposals[pdx].Value)
									found = true
									break
								}
							}
						} else {
							st, ver := getState(proTID)
							if st == kvrows.CommittedState {
								if ver <= maxVer {
									keys, vals = appendKeysVals(keys, vals, k,
										proposals[len(proposals)-1].Value)
									found = true
								}
							} else if st != kvrows.AbortedState {
								blockingProposal = true
								return &kvrows.ErrBlockingProposal{
									TID: proTID,
									Key: k.Copy(),
								}
							}

						}

						return nil
					})
				if err != nil {
					if blockingProposal {
						return keys, vals, k.Copy().SQLKey, nil
					}
					return nil, nil, nil, err
				}
				if found {
					break
				}
			} else if k.Version <= maxVer {
				err := w.Value(
					func(val []byte) error {
						keys, vals = appendKeysVals(keys, vals, k, val)
						return nil
					})
				if err != nil {
					return nil, nil, nil, err
				}
				break
			}

			kbuf, ok := w.Next()
			if !ok {
				break
			}
			k, ok = kvrows.ParseKey(kbuf)
			if !ok {
				return nil, nil, nil, fmt.Errorf("localkv: unable to parse key %v", kbuf)
			}
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
			if !bytes.Equal(k.SQLKey, nxt.SQLKey) {
				k = nxt
				break
			}
		}
	}

	return keys, vals, k.Copy().SQLKey, nil
}

func (lkv localKV) ModifyRelation(ctx context.Context, getState kvrows.GetTxState,
	tid kvrows.TransactionID, sid, mid uint64, keys []kvrows.Key, vals [][]byte) error {

	// XXX
	return errors.New("not implemented")
}

func (lkv localKV) InsertRelation(ctx context.Context, getState kvrows.GetTxState,
	tid kvrows.TransactionID, sid, mid uint64, keys [][]byte, vals [][]byte) error {

	// XXX
	return errors.New("not implemented")
}

/*
func (lkv localKV) nextKey(getState kvrows.GetTxState, txCtx kvrows.TxContext, w Walker,
	k kvrows.Key, maxVer uint64, fkv func(key kvrows.Key, val []byte) error) (kvrows.Key, bool,
	error) {

	blockingProposal := false
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

					if txk.Equal(txCtx.TxKey) {
						if k.Version < txCtx.SID {
							found = true
						}
					} else {
						st, ver := getState(txk)
						if st == kvrows.CommittedState {
							if ver <= maxVer {
								found = true
							}
						} else if st != kvrows.AbortedState {
							blockingProposal = true
							return &kvrows.ErrBlockingProposal{
								TxKey: txk.Copy(),
								Key:   k.Copy(),
							}
						}
					}

					if found {
						return fkv(k, pval)
					}
					return nil
				})
			if found || err != nil {
				return k, blockingProposal, err
			}
		} else if k.Type == kvrows.DurableKeyType && k.Version <= maxVer {
			return k, false, w.Value(
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
			return kvrows.Key{}, false, fmt.Errorf("localkv: unable to parse key %v", kbuf)
		}
	}
	return k, false, io.EOF
}

func (lkv localKV) ScanRelation(ctx context.Context, getState kvrows.GetTxState,
	txCtx kvrows.TxContext, mid, maxVer uint64, num int, seek []byte) ([]kvrows.Key, [][]byte,
	[]byte, error) {

	tx, err := lkv.st.Begin(false)
	if err != nil {
		return nil, nil, nil, err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return nil, nil, nil, err
	}

	w := m.Walk(nil)
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
		var blockingProposal bool

		// Scan and find the greatest version of a key or a proposal key.
		k, blockingProposal, err = lkv.nextKey(getState, txCtx, w, k, maxVer,
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
			if blockingProposal {
				return keys, vals, k.Copy().Key, err
			}
			return nil, nil, nil, err
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

func (lkv localKV) modifyRelation(getState kvrows.GetTxState, txCtx kvrows.TxContext,
	modifyKey kvrows.Key, val []byte, m Mapper) error {

	w := m.Walk(modifyKey.Key)
	defer w.Close()

	kbuf, ok := w.Seek(modifyKey.Key)
	if !ok {
		return fmt.Errorf("localkv: key %v not found", modifyKey)
	}

	// The key exists; check to see if it has a visible value.
	k, ok := kvrows.ParseKey(kbuf)
	if !ok {
		return fmt.Errorf("localkv: unable to parse key %v", kbuf)
	}

	// Scan and find the greatest version of the key, if it has a visible value.
	_, _, err := lkv.nextKey(getState, txCtx, w, k, kvrows.MaximumVersion,
		func(key kvrows.Key, val []byte) error {
			if len(val) == 0 || val[0] == kvrows.TombstoneValue {
				return fmt.Errorf("localkv: key %v not found", modifyKey)
			}
			if !key.Equal(modifyKey) {
				return fmt.Errorf("localkv: key %v has conflicting write", modifyKey)
			}
			return nil
		})
	if err != nil {
		return err
	}

	// The key has a visible value; go ahead and propose an updated value.
	return m.Set(kvrows.MakeProposalKey(modifyKey.Key, txCtx.SID).Encode(),
		kvrows.MakeProposalValue(txCtx.TxKey, val))
}

func (lkv localKV) ModifyRelation(ctx context.Context, getState kvrows.GetTxState,
	txCtx kvrows.TxContext, mid uint64, keys []kvrows.Key, vals [][]byte) error {

	tx, err := lkv.st.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return err
	}

	var val []byte
	if vals == nil {
		val = kvrows.MakeTombstoneValue()
	}

	for idx := range keys {
		if vals != nil {
			val = vals[idx]
		}
		err := lkv.modifyRelation(getState, txCtx, keys[idx], val, m)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (lkv localKV) insertRelation(getState kvrows.GetTxState, txCtx kvrows.TxContext,
	insertKey []byte, val []byte, m Mapper) error {

	w := m.Walk(insertKey)
	defer w.Close()

	kbuf, ok := w.Seek(insertKey)
	if ok {
		// The key exists; check to see if it has a visible value.
		k, ok := kvrows.ParseKey(kbuf)
		if !ok {
			return fmt.Errorf("localkv: unable to parse key %v", kbuf)
		}

		// Scan and find the greatest version of the key, if it has a visible value.
		_, _, err := lkv.nextKey(getState, txCtx, w, k, kvrows.MaximumVersion,
			func(key kvrows.Key, val []byte) error {
				if len(val) == 0 || val[0] == kvrows.TombstoneValue {
					return nil
				}
				return fmt.Errorf("localkv: existing row for key %v", key)
			})
		if err != nil {
			return err
		}
	}

	// No value is visible for the key; go ahead and propose a value.
	return m.Set(kvrows.MakeProposalKey(insertKey, txCtx.SID).Encode(),
		kvrows.MakeProposalValue(txCtx.TxKey, val))
}

func (lkv localKV) InsertRelation(ctx context.Context, getState kvrows.GetTxState,
	txCtx kvrows.TxContext, mid uint64, keys [][]byte, vals [][]byte) error {

	tx, err := lkv.st.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return err
	}

	for idx := range keys {
		err := lkv.insertRelation(getState, txCtx, keys[idx], vals[idx], m)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
*/

func (lkv localKV) CleanKey(ctx context.Context, getState kvrows.GetTxState, mid uint64,
	key []byte) error {

	// XXX
	return errors.New("not implemented")
}

func (lkv localKV) CleanRelation(ctx context.Context, getState kvrows.GetTxState, mid uint64,
	start []byte, max int) ([]byte, error) {

	// XXX
	return nil, errors.New("not implemented")
}
