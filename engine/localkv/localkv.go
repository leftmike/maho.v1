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
						return keys, vals, k.Copy().SQLKey, err
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

func modifyRelation(getState kvrows.GetTxState, tid kvrows.TransactionID, sid uint64,
	modifyKey kvrows.Key, newVal []byte, m Mapper) error {

	w := m.Walk(modifyKey.SQLKey)
	defer w.Close()

	kbuf, ok := w.Seek(modifyKey.SQLKey)
	if !ok {
		return fmt.Errorf("localkv: no value for key %v", modifyKey.SQLKey)
	}

	k, ok := kvrows.ParseKey(kbuf)
	if !ok {
		return fmt.Errorf("localkv: unable to parse key %v", kbuf)
	}

	if k.Version == kvrows.ProposalVersion && modifyKey.Version == kvrows.ProposalVersion {
		found := false
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
					v := proposals[len(proposals)-1].Value
					if proposals[len(proposals)-1].SID == sid {
						return fmt.Errorf("localkv: conflicting modification of key %v",
							modifyKey.SQLKey)
					} else if len(v) == 0 || v[0] == kvrows.TombstoneValue {
						return fmt.Errorf("localkv: no value for key %v", modifyKey.SQLKey)
					} else {
						found = true

						// Go ahead and propose a modification to the key.
						return appendProposal(tid, sid, modifyKey.SQLKey, newVal, proposals, m)
					}
				} else {
					st, ver := getState(proTID)
					if st == kvrows.CommittedState {
						v := proposals[len(proposals)-1].Value
						if len(v) == 0 || v[0] == kvrows.TombstoneValue {
							return fmt.Errorf("localkv: no value for key %v", modifyKey.SQLKey)
						} else {
							found = true

							// Go ahead and propose a modification to the key, but
							// first, make the previous, committed, proposal durable.

							panic("committed proposal; value is a tombstone") // XXX
							err := m.Set(kvrows.Key{modifyKey.SQLKey, ver}.Encode(), v)
							if err != nil {
								return err
							}
							return appendProposal(tid, sid, modifyKey.SQLKey, newVal, proposals, m)
						}
					} else if st != kvrows.AbortedState {
						return &kvrows.ErrBlockingProposal{
							TID: proTID,
							Key: k.Copy(),
						}
					}
				}

				return nil
			})

		if err != nil || found {
			return err
		}

		kbuf, ok := w.Next()
		if !ok {
			return fmt.Errorf("localkv: no value for key %v", modifyKey.SQLKey)
		}
		k, ok = kvrows.ParseKey(kbuf)
		if !ok {
			return fmt.Errorf("localkv: unable to parse key %v", kbuf)
		}
	}

	if k.Version != modifyKey.Version {
		return fmt.Errorf("localkv: conflicting modification of key %v", modifyKey.SQLKey)
	}

	err := w.Value(
		func(val []byte) error {
			if len(val) == 0 || val[0] == kvrows.TombstoneValue {
				return fmt.Errorf("localkv: no value for key %v", modifyKey.SQLKey)
			}

			return nil
		})
	if err != nil {
		return err
	}

	// Propose a modification.
	return appendProposal(tid, sid, modifyKey.SQLKey, newVal, nil, m)
}

func (lkv localKV) ModifyRelation(ctx context.Context, getState kvrows.GetTxState,
	tid kvrows.TransactionID, sid, mid uint64, keys []kvrows.Key, vals [][]byte) error {

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
		err := modifyRelation(getState, tid, sid, keys[idx], val, m)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func appendProposal(tid kvrows.TransactionID, sid uint64, insertKey, val []byte,
	proposals []kvrows.Proposal, m Mapper) error {

	return m.Set(kvrows.Key{insertKey, kvrows.ProposalVersion}.Encode(),
		kvrows.MakeProposalValue(tid, append(proposals, kvrows.Proposal{sid, val})))
}

func insertRelation(getState kvrows.GetTxState, tid kvrows.TransactionID, sid uint64,
	insertKey []byte, newVal []byte, m Mapper) error {

	w := m.Walk(insertKey)
	defer w.Close()

	kbuf, ok := w.Seek(insertKey)
	if !ok {
		// No value for the key; go ahead and propose a value.
		return appendProposal(tid, sid, insertKey, newVal, nil, m)
	}

	k, ok := kvrows.ParseKey(kbuf)
	if !ok {
		return fmt.Errorf("localkv: unable to parse key %v", kbuf)
	}

	if k.Version == kvrows.ProposalVersion {
		found := false
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
					v := proposals[len(proposals)-1].Value
					if len(v) == 0 || v[0] != kvrows.TombstoneValue ||
						proposals[len(proposals)-1].SID == sid {

						return fmt.Errorf("localkv: existing value for key %v", insertKey)
					} else {
						found = true

						// No visible value for the key; go ahead and propose a value.
						return appendProposal(tid, sid, insertKey, newVal, proposals, m)
					}
				} else {
					st, ver := getState(proTID)
					if st == kvrows.CommittedState {
						v := proposals[len(proposals)-1].Value
						if len(v) == 0 || v[0] != kvrows.TombstoneValue {
							return fmt.Errorf("localkv: existing value for key %v", insertKey)
						} else {
							found = true

							// No visible value for the key; go ahead and propose a value, but
							// first, make the previous, committed, proposal durable.

							panic("committed proposal; value is a tombstone") // XXX
							err := m.Set(kvrows.Key{insertKey, ver}.Encode(), v)
							if err != nil {
								return err
							}
							return appendProposal(tid, sid, insertKey, newVal, proposals, m)
						}
					} else if st != kvrows.AbortedState {
						return &kvrows.ErrBlockingProposal{
							TID: proTID,
							Key: k.Copy(),
						}
					}
				}

				return nil
			})

		if err != nil || found {
			return err
		}

		kbuf, ok := w.Next()
		if !ok {
			// No visible value for the key; go ahead and propose a value.
			return appendProposal(tid, sid, insertKey, newVal, nil, m)
		}
		k, ok = kvrows.ParseKey(kbuf)
		if !ok {
			return fmt.Errorf("localkv: unable to parse key %v", kbuf)
		}
	}

	err := w.Value(
		func(val []byte) error {
			if len(val) == 0 || val[0] == kvrows.TombstoneValue {
				return nil
			}

			return fmt.Errorf("localkv: existing value for key %v", insertKey)
		})
	if err != nil {
		return err
	}

	// No visible value for the key; go ahead and propose a value.
	return appendProposal(tid, sid, insertKey, newVal, nil, m)
}

func (lkv localKV) InsertRelation(ctx context.Context, getState kvrows.GetTxState,
	tid kvrows.TransactionID, sid, mid uint64, keys [][]byte, vals [][]byte) error {

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
		err := insertRelation(getState, tid, sid, keys[idx], vals[idx], m)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func cleanKey(getState kvrows.GetTxState, kbuf []byte, m Mapper, w Walker) error {
	k, ok := kvrows.ParseKey(kbuf)
	if !ok {
		// XXX: delete the key and log an error
		return fmt.Errorf("localkv: unable to parse key %v", kbuf)
	}

	if k.Version != kvrows.ProposalVersion {
		return nil
	}

	return w.Value(
		func(val []byte) error {
			if len(val) == 0 || val[0] != kvrows.ProposalValue {
				// XXX: delete the key and log an error
				return fmt.Errorf("localkv: unable to parse value for key %v: %v", k,
					val)
			}
			tid, proposals, ok := kvrows.ParseProposalValue(val)
			if !ok {
				// XXX: delete the key and log an error
				return fmt.Errorf("localkv: unable to parse value for key %v: %v", k,
					val)
			}

			st, ver := getState(tid)
			if st == kvrows.CommittedState {
				v := proposals[len(proposals)-1].Value
				return m.Set(kvrows.Key{k.SQLKey, ver}.Encode(), v)
			} else if st == kvrows.AbortedState {
				return w.Delete()
			}
			return nil
		})
}

func (lkv localKV) CleanKeys(ctx context.Context, getState kvrows.GetTxState, mid uint64,
	keys [][]byte) error {

	tx, err := lkv.st.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return err
	}

	w := m.Walk(nil)
	defer w.Close()

	for _, key := range keys {
		kbuf, ok := w.Seek(key)
		if !ok {
			return nil
		}

		err := cleanKey(getState, kbuf, m, w)
		if err != nil {
			return err
		}
	}

	w.Close()
	return tx.Commit()
}

func cleanRelation(getState kvrows.GetTxState, start []byte, num int, m Mapper) ([]byte, error) {
	w := m.Walk(nil)
	defer w.Close()

	kbuf, ok := w.Rewind()
	if !ok {
		return nil, nil
	}

	for num > 0 {
		err := cleanKey(getState, kbuf, m, w)
		if err != nil {
			return nil, err
		}
		kbuf, ok = w.Next()
		if !ok {
			return nil, nil
		}
		num -= 1
	}

	k, ok := kvrows.ParseKey(kbuf)
	if !ok {
		// XXX: should clean up the key and continue with cleaning the relation
		return nil, nil
	}

	return k.Copy().SQLKey, nil
}

func (lkv localKV) CleanRelation(ctx context.Context, getState kvrows.GetTxState, mid uint64,
	start []byte, num int) ([]byte, error) {

	tx, err := lkv.st.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return nil, err
	}

	start, err = cleanRelation(getState, start, num, m)
	if err != nil {
		return nil, err
	}

	return start, tx.Commit()
}
