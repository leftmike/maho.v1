package localkv

import (
	"bytes"
	"context"
	"fmt"

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

func (lkv localKV) ReadValue(ctx context.Context, mid uint64, key []byte) (uint64, []byte, error) {
	kvrows.MustBeMetadataKey(key)

	tx, err := lkv.st.Begin(false)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return 0, nil, err
	}

	w := m.Walk(key)
	defer w.Close()

	kbuf, ok := w.Rewind()
	if !ok {
		return 0, nil, kvrows.ErrKeyNotFound
	}

	_, ver, ok := kvrows.ParseKey(kbuf)
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

	return ver, retval, nil
}

func (lkv localKV) ListValues(ctx context.Context, mid uint64,
	listKeyValue kvrows.ListKeyValue) error {

	tx, err := lkv.st.Begin(false)
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

	kbuf, ok := w.Rewind()

	for ok {
		var k []byte
		var ver uint64
		k, ver, ok = kvrows.ParseKey(kbuf)
		if !ok {
			return fmt.Errorf("localkv: unable to parse key %v", kbuf)
		}

		var done bool
		err = w.Value(
			func(val []byte) error {
				var err error
				done, err = listKeyValue(k, ver, val)
				return err
			})
		if err != nil {
			return err
		}
		if done {
			break
		}

		kbuf, ok = w.Next()
	}

	return nil
}

func (lkv localKV) WriteValue(ctx context.Context, mid uint64, key []byte, ver uint64,
	val []byte) error {

	kvrows.MustBeMetadataKey(key)

	tx, err := lkv.st.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return err
	}

	w := m.Walk(key)
	defer w.Close()

	buf, ok := w.Rewind()
	if !ok {
		if ver > 0 {
			return kvrows.ErrKeyNotFound
		}
	} else {
		_, kv, ok := kvrows.ParseKey(buf)
		if !ok {
			return fmt.Errorf("localkv: unable to parse key %v", buf)
		}
		if kv != ver {
			return kvrows.ErrValueVersionMismatch
		}

		err = w.Delete()
		if err != nil {
			return err
		}
	}

	err = m.Set(kvrows.MakeKeyVersion(key, ver+1), val)
	if err != nil {
		return err
	}

	w.Close()
	return tx.Commit()
}

func (lkv localKV) ScanMap(ctx context.Context, getState kvrows.GetTxState,
	tid kvrows.TransactionID, sid, mid uint64, seek []byte,
	scanKeyValue kvrows.ScanKeyValue) (next []byte, err error) {

	tx, err := lkv.st.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return nil, err
	}

	w := m.Walk(nil)
	defer w.Close()

	var kbuf []byte
	var ok bool
	if seek == nil {
		kbuf, ok = w.Rewind()
		if !ok {
			return nil, nil
		}

		// Skip over the metadata keys at the beginning of the map.
		for kbuf[0] == 0 {
			kbuf, ok = w.Next()
			if !ok {
				return nil, nil
			}
		}
	} else {
		kvrows.MustBeSQLKey(seek)

		kbuf, ok = w.Seek(seek)
		if !ok {
			return nil, nil
		}
	}

	k, kv, ok := kvrows.ParseKey(kbuf)
	if !ok {
		return nil, fmt.Errorf("localkv: unable to parse key %v", kbuf)
	}

	var done bool
	for !done {
		for {
			if kv == kvrows.ProposalVersion {
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
							for pdx := len(proposals) - 1; pdx >= 0; pdx -= 1 {
								if proposals[pdx].SID < sid {
									found = true
									proVal := proposals[pdx].Value
									if len(proVal) > 0 && proVal[0] != kvrows.TombstoneValue {
										done, err = scanKeyValue(k, kvrows.ProposalVersion, proVal)
										return err
									}
									break
								}
							}
						} else {
							st, ver := getState(proTID)
							if st == kvrows.CommittedState {
								found = true
								proVal := proposals[len(proposals)-1].Value
								if len(proVal) > 0 && proVal[0] != kvrows.TombstoneValue {
									done, err = scanKeyValue(k, ver, proVal)
									return err
								}
							} else if st != kvrows.AbortedState {
								return &kvrows.ErrBlockingProposal{
									TID: proTID,
									Key: kvrows.CopyKey(k),
								}
							}
						}

						return nil
					})
				if err != nil {
					return nil, err
				}
				if found {
					break
				}
			} else {
				err := w.Value(
					func(val []byte) error {
						if len(val) > 0 && val[0] != kvrows.TombstoneValue {
							var err error
							done, err = scanKeyValue(k, kv, val)
							return err
						}
						return nil
					})
				if err != nil {
					return nil, err
				}
				break
			}

			kbuf, ok := w.Next()
			if !ok {
				return nil, nil
			}
			k, kv, ok = kvrows.ParseKey(kbuf)
			if !ok {
				return nil, fmt.Errorf("localkv: unable to parse key %v", kbuf)
			}
		}

		// Skip along to the next key.
		for {
			kbuf, ok = w.Next()
			if !ok {
				return nil, nil
			}
			nxt, nv, ok := kvrows.ParseKey(kbuf)
			if !ok {
				return nil, fmt.Errorf("localkv: unable to parse key %v", kbuf)
			}
			if !bytes.Equal(k, nxt) {
				k = nxt
				kv = nv
				break
			}
		}
	}

	return kvrows.CopyKey(k), nil
}

func appendProposal(tid kvrows.TransactionID, sid uint64, insertKey, val []byte,
	proposals []kvrows.Proposal, m Mapper) error {

	return m.Set(kvrows.MakeKeyVersion(insertKey, kvrows.ProposalVersion),
		kvrows.MakeProposalValue(tid, append(proposals, kvrows.Proposal{sid, val})))
}

func modifyMap(getState kvrows.GetTxState, tid kvrows.TransactionID, sid uint64,
	key []byte, ver uint64, modifyKeyValue kvrows.ModifyKeyValue, m Mapper) error {

	w := m.Walk(key)
	defer w.Close()

	kbuf, ok := w.Seek(key)
	if !ok {
		return fmt.Errorf("localkv: no value for key %v", key)
	}

	k, kv, ok := kvrows.ParseKey(kbuf)
	if !ok {
		return fmt.Errorf("localkv: unable to parse key %v", kbuf)
	}

	if kv == kvrows.ProposalVersion {
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

				if ver == kvrows.ProposalVersion {
					if proTID != tid || proposals[len(proposals)-1].SID == sid {
						return fmt.Errorf("localkv: conflicting modification of key %v", key)
					}

					// Go ahead and propose a modification to the key.
					found = true
					newVal, err := modifyKeyValue(key, ver, proposals[len(proposals)-1].Value)
					if err != nil {
						return err
					}
					return appendProposal(tid, sid, key, newVal, proposals, m)
				} else {
					st, curVer := getState(proTID)
					if st == kvrows.CommittedState {
						if ver != curVer {
							return fmt.Errorf("localkv: conflicting modification of key %v",
								key)
						}

						// Go ahead and propose a modification to the key, but
						// first, make the previous, committed, proposal durable.

						v := proposals[len(proposals)-1].Value
						err := m.Set(kvrows.MakeKeyVersion(key, curVer), v)
						if err != nil {
							return err
						}

						found = true
						newVal, err := modifyKeyValue(key, ver, v)
						if err != nil {
							return err
						}
						return appendProposal(tid, sid, key, newVal, nil, m)
					} else if st != kvrows.AbortedState {
						return &kvrows.ErrBlockingProposal{
							TID: proTID,
							Key: kvrows.CopyKey(k),
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
			return fmt.Errorf("localkv: no value for key %v", key)
		}

		k, kv, ok = kvrows.ParseKey(kbuf)
		if !ok {
			return fmt.Errorf("localkv: unable to parse key %v", kbuf)
		}
	}

	if kv != ver {
		return fmt.Errorf("localkv: conflicting modification of key %v", key)
	}

	// Propose a modification.
	return w.Value(
		func(val []byte) error {
			newVal, err := modifyKeyValue(key, ver, val)
			if err != nil {
				return err
			}
			return appendProposal(tid, sid, key, newVal, nil, m)
		})
}

func (lkv localKV) ModifyMap(ctx context.Context, getState kvrows.GetTxState,
	tid kvrows.TransactionID, sid, mid uint64, key []byte, ver uint64,
	modifyKeyValue kvrows.ModifyKeyValue) error {

	kvrows.MustBeSQLKey(key)

	tx, err := lkv.st.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return err
	}

	err = modifyMap(getState, tid, sid, key, ver, modifyKeyValue, m)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func deleteKeyValue(key []byte, ver uint64, val []byte) ([]byte, error) {
	return kvrows.MakeTombstoneValue(), nil
}

func (lkv localKV) DeleteMap(ctx context.Context, getState kvrows.GetTxState,
	tid kvrows.TransactionID, sid, mid uint64, key []byte, ver uint64) error {

	kvrows.MustBeSQLKey(key)

	tx, err := lkv.st.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return err
	}

	err = modifyMap(getState, tid, sid, key, ver, deleteKeyValue, m)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func insertMap(getState kvrows.GetTxState, tid kvrows.TransactionID, sid uint64,
	insertKey []byte, newVal []byte, m Mapper) error {

	w := m.Walk(insertKey)
	defer w.Close()

	kbuf, ok := w.Seek(insertKey)
	if !ok {
		// No value for the key; go ahead and propose a value.
		return appendProposal(tid, sid, insertKey, newVal, nil, m)
	}

	k, kv, ok := kvrows.ParseKey(kbuf)
	if !ok {
		return fmt.Errorf("localkv: unable to parse key %v", kbuf)
	}

	if kv == kvrows.ProposalVersion {
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

							err := m.Set(kvrows.MakeKeyVersion(insertKey, ver), v)
							if err != nil {
								return err
							}
							return appendProposal(tid, sid, insertKey, newVal, nil, m)
						}
					} else if st != kvrows.AbortedState {
						return &kvrows.ErrBlockingProposal{
							TID: proTID,
							Key: kvrows.CopyKey(k),
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

		k, kv, ok = kvrows.ParseKey(kbuf)
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

func (lkv localKV) InsertMap(ctx context.Context, getState kvrows.GetTxState,
	tid kvrows.TransactionID, sid, mid uint64, key, val []byte) error {

	kvrows.MustBeSQLKey(key)

	tx, err := lkv.st.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return err
	}

	err = insertMap(getState, tid, sid, key, val, m)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func cleanKey(getState kvrows.GetTxState, kbuf []byte, bad bool, m Mapper, w Walker) error {
	k, kv, ok := kvrows.ParseKey(kbuf)
	if !ok {
		if bad {
			// XXX: log cleaning up the bad key
			return w.Delete()
		}
		return fmt.Errorf("localkv: unable to parse key %v", kbuf)
	}

	if kv != kvrows.ProposalVersion {
		return nil
	}

	return w.Value(
		func(val []byte) error {
			if len(val) == 0 || val[0] != kvrows.ProposalValue {
				if bad {
					// XXX: log cleaning up the bad proposal
					return w.Delete()
				}
				return fmt.Errorf("localkv: unable to parse value for key %v: %v", k,
					val)
			}
			tid, proposals, ok := kvrows.ParseProposalValue(val)
			if !ok {
				if bad {
					// XXX: log cleaning up the bad proposal
					return w.Delete()
				}
				return fmt.Errorf("localkv: unable to parse value for key %v: %v", k,
					val)
			}

			st, ver := getState(tid)
			if st == kvrows.CommittedState {
				err := w.Delete()
				if err != nil {
					return err
				}
				v := proposals[len(proposals)-1].Value
				return m.Set(kvrows.MakeKeyVersion(k, ver), v)
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

		err := cleanKey(getState, kbuf, false, m, w)
		if err != nil {
			return err
		}
	}

	w.Close()
	return tx.Commit()
}

func cleanMap(getState kvrows.GetTxState, bad bool, m Mapper) error {
	w := m.Walk(nil)
	defer w.Close()

	kbuf, ok := w.Rewind()
	if !ok {
		return nil
	}

	for {
		if kbuf[0] == 0 {
			// Metadata keys don't need to be cleaned.
			continue
		}

		err := cleanKey(getState, kbuf, bad, m, w)
		if err != nil {
			return err
		}
		kbuf, ok = w.Next()
		if !ok {
			break
		}
	}
	return nil
}

func (lkv localKV) CleanMap(ctx context.Context, getState kvrows.GetTxState,
	mid uint64, bad bool) error {

	tx, err := lkv.st.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		return err
	}

	err = cleanMap(getState, bad, m)
	if err != nil {
		return err
	}

	return tx.Commit()
}
