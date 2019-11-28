package localkv_test

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/engine/badger"
	"github.com/leftmike/maho/engine/bbolt"
	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/localkv"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

const (
	cmdReadValue  = 1
	cmdListValues = 2
	cmdWriteValue = 3
)

type cmd struct {
	cmd  int
	key  kvrows.Key
	val  []byte
	keys []kvrows.Key
	vals [][]byte
	ver  uint64
	fail bool
}

func runCmds(t *testing.T, st kvrows.Store, mid uint64, cmds []cmd) {
	ctx := context.Background()

	for i, cmd := range cmds {
		switch cmd.cmd {
		case cmdReadValue:
			ver, val, err := st.ReadValue(ctx, mid, cmd.key)
			if err != nil {
				if !cmd.fail {
					t.Errorf("ReadValue(%d, %v) failed with %s", i, cmd.key, err)
				}
			} else {
				if cmd.fail {
					t.Errorf("ReadValue(%d, %v) did not fail", i, cmd.key)
				} else {
					if ver != cmd.ver {
						t.Errorf("ReadValue(%d, %v) got version %d; want %d", i, cmd.key, ver,
							cmd.ver)
					}
					if bytes.Compare(val, cmd.val) != 0 {
						t.Errorf("ReadValue(%d, %v) got value %v; want %v", i, cmd.key, val,
							cmd.val)
					}
				}
			}
		case cmdListValues:
			keys, vals, err := st.ListValues(ctx, mid)
			if err != nil {
				if !cmd.fail {
					t.Errorf("ListValues(%d) failed with %s", i, err)
				}
			} else {
				if cmd.fail {
					t.Errorf("ListValues(%d) did not fail", i)
				} else {
					if !testutil.DeepEqual(keys, cmd.keys) {
						t.Errorf("ListValues(%d) got %v for keys; want %v", i, keys, cmd.keys)
					}
					if !testutil.DeepEqual(vals, cmd.vals) {
						t.Errorf("ListValues(%d) got %v for vales; want %v", i, vals, cmd.vals)
					}
				}
			}
		case cmdWriteValue:
			err := st.WriteValue(ctx, mid, cmd.key, cmd.ver, cmd.val)
			if err != nil {
				if !cmd.fail {
					t.Errorf("WriteValue(%d, %v) failed with %s", i, cmd.key, err)
				}
			} else if cmd.fail {
				t.Errorf("WriteValue(%d, %v) did not fail", i, cmd.key)
			}
		default:
			t.Fatal("unexpected test command")
		}
	}
}

func testReadWriteList(t *testing.T, st kvrows.Store) {
	t.Helper()

	runCmds(t, st, 1, []cmd{
		{
			cmd:  cmdReadValue,
			key:  kvrows.Key{Key: []byte("abcd"), Type: kvrows.TransactionKeyType},
			fail: true,
		},
		{
			cmd: cmdListValues,
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{Key: []byte("abcd"), Type: kvrows.TransactionKeyType},
			ver: 1,
			val: []byte("1234567890"),
		},
		{
			cmd: cmdReadValue,
			key: kvrows.Key{Key: []byte("abcd"), Type: kvrows.TransactionKeyType},
			ver: 1,
			val: []byte("1234567890"),
		},
		{
			cmd: cmdListValues,
			keys: []kvrows.Key{
				{Key: []byte("abcd"), Version: 1, Type: kvrows.TransactionKeyType},
			},
			vals: [][]byte{
				[]byte("1234567890"),
			},
		},
		{
			cmd:  cmdWriteValue,
			key:  kvrows.Key{Key: []byte("abcd"), Type: kvrows.TransactionKeyType},
			ver:  2,
			val:  []byte("0987654321"),
			fail: true,
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{Key: []byte("abcd"), Version: 1, Type: kvrows.TransactionKeyType},
			ver: 2,
			val: []byte("0987654321"),
		},
		{
			cmd: cmdReadValue,
			key: kvrows.Key{Key: []byte("abcd"), Type: kvrows.TransactionKeyType},
			ver: 2,
			val: []byte("0987654321"),
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{Key: []byte("abcd"), Version: 2, Type: kvrows.TransactionKeyType},
			ver: 10,
			val: []byte("1234567890987654321"),
		},
		{
			cmd:  cmdWriteValue,
			key:  kvrows.Key{Key: []byte("abcd"), Version: 10, Type: kvrows.TransactionKeyType},
			ver:  5,
			val:  []byte("0000000000"),
			fail: true,
		},
		{
			cmd: cmdReadValue,
			key: kvrows.Key{Key: []byte("abcd"), Type: kvrows.TransactionKeyType},
			ver: 10,
			val: []byte("1234567890987654321"),
		},
		{
			cmd: cmdListValues,
			keys: []kvrows.Key{
				{Key: []byte("abcd"), Version: 10, Type: kvrows.TransactionKeyType},
			},
			vals: [][]byte{
				[]byte("1234567890987654321"),
			},
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{Key: []byte("abcd1"), Type: kvrows.TransactionKeyType},
			ver: 1,
			val: []byte("one"),
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{Key: []byte("abcd2"), Type: kvrows.TransactionKeyType},
			ver: 2,
			val: []byte("two"),
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{Key: []byte("abcd3"), Type: kvrows.TransactionKeyType},
			ver: 3,
			val: []byte("three"),
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{Key: []byte("abcd4"), Type: kvrows.TransactionKeyType},
			ver: 4,
			val: []byte("four"),
		},
		{
			cmd: cmdListValues,
			keys: []kvrows.Key{
				{Key: []byte("abcd"), Version: 10, Type: kvrows.TransactionKeyType},
				{Key: []byte("abcd1"), Version: 1, Type: kvrows.TransactionKeyType},
				{Key: []byte("abcd2"), Version: 2, Type: kvrows.TransactionKeyType},
				{Key: []byte("abcd3"), Version: 3, Type: kvrows.TransactionKeyType},
				{Key: []byte("abcd4"), Version: 4, Type: kvrows.TransactionKeyType},
			},
			vals: [][]byte{
				[]byte("1234567890987654321"),
				[]byte("one"),
				[]byte("two"),
				[]byte("three"),
				[]byte("four"),
			},
		},
	})
}

type keyValue struct {
	key     kvrows.Key
	val     []byte
	visible bool
}

func initializeMap(t *testing.T, st localkv.Store, mid uint64, keyVals []keyValue) {
	t.Helper()

	tx, err := st.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	m, err := tx.Map(mid)
	if err != nil {
		t.Fatal(err)
	}

	var prevKey []byte
	for _, kv := range keyVals {
		k := kv.key.Encode()
		if bytes.Compare(prevKey, k) >= 0 {
			t.Fatalf("initializeMap: keys out of order: %v %v", prevKey, k)
		}

		err = m.Set(k, kv.val)
		if err != nil {
			t.Fatal(err)
		}
		prevKey = k
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
}

func countVisible(keyVals []keyValue) int {
	cnt := 0
	for _, kv := range keyVals {
		if kv.visible {
			cnt += 1
		}
	}
	return cnt
}

func checkScan(t *testing.T, keyVals []keyValue, idx int, keys []kvrows.Key, vals [][]byte) int {
	if len(keys) != len(vals) {
		t.Errorf("ScanRelation: got %d keys and %d values", len(keys), len(vals))
	}

	for jdx := range keys {
		for {
			if idx >= len(keyVals) {
				t.Fatalf("ScanRelation: too many keys: %d", len(keys))
			}
			if keyVals[idx].visible {
				break
			}
			idx += 1
		}

		if !testutil.DeepEqual(keys[jdx], keyVals[idx].key) {
			t.Errorf("ScanRelation: got %v key; wanted %v", keys[jdx], keyVals[idx].key)
		}
		if keyVals[idx].key.Type == kvrows.ProposalKeyType {
			_, buf, ok := kvrows.ParseProposalValue(keyVals[idx].val)
			if !ok {
				t.Fatalf("ScanRelation: expected proposal value with proposal key: %v; got %v",
					keyVals[idx].key, keyVals[idx].val)
			}
			if !bytes.Equal(vals[jdx], buf) {
				t.Errorf("ScanRelation: got %v value; wanted %v", vals[jdx], buf)
			}
		} else if !bytes.Equal(vals[jdx], keyVals[idx].val) {
			t.Errorf("ScanRelation: got %v value; wanted %v", vals[jdx], keyVals[idx].val)
		}
		idx += 1
	}

	return idx
}

type relation struct {
	txKey kvrows.TransactionKey
	mid   uint64
	sid   uint64
	state kvrows.TransactionState
}

func (rel relation) TxKey() kvrows.TransactionKey {
	return rel.txKey
}

func (rel relation) CurrentStatement() uint64 {
	return rel.sid
}

func (rel relation) MapID() uint64 {
	return rel.mid
}

func (rel relation) GetTransactionState(txKey kvrows.TransactionKey) kvrows.TransactionState {
	return rel.state
}

func testScanRelation(t *testing.T, st localkv.Store) {
	ctx := context.Background()
	lkv := localkv.NewStore(st)

	txk := kvrows.TransactionKey{
		MID:   1000,
		Key:   []byte("cccc"),
		TID:   99,
		Epoch: 888,
	}
	keyVals := []keyValue{
		{
			key: kvrows.TransactionKey{
				MID:   1000,
				Key:   []byte("aaaa"),
				TID:   11111,
				Epoch: 1234,
			}.EncodeKey(),
			val: []byte("mid: 1000, key: aaaa, tid: 11111, epoch: 1234"),
		},
		{
			key:     kvrows.Key{[]byte("bbbb"), 50, kvrows.DurableKeyType},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb@50")}),
			visible: true,
		},
		{
			key: kvrows.Key{[]byte("bbbb"), 49, kvrows.DurableKeyType},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb@49")}),
		},
		{
			key: kvrows.Key{[]byte("bbbb"), 48, kvrows.DurableKeyType},
			val: kvrows.MakeTombstoneValue(),
		},
		{
			key: kvrows.Key{[]byte("bbbb"), 47, kvrows.DurableKeyType},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb@47")}),
		},
		{
			key: txk.EncodeKey(),
			val: []byte("mid: 1000, key: cccc, tid: 99, epoch: 888"),
		},
		{
			key:     kvrows.Key{[]byte("dddd"), 100, kvrows.DurableKeyType},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("dddd@100")}),
			visible: true,
		},
		{
			key: kvrows.Key{[]byte("eeee"), 10, kvrows.DurableKeyType},
			val: kvrows.MakeTombstoneValue(),
		},
		{
			key: kvrows.Key{[]byte("eeee"), 9, kvrows.DurableKeyType},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee@9")}),
		},
		{
			key: kvrows.Key{[]byte("eeee"), 8, kvrows.DurableKeyType},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee@8")}),
		},
		{
			key:     kvrows.Key{[]byte("ffff"), 12, kvrows.ProposalKeyType},
			val:     kvrows.MakeProposalValue(txk, []byte("proposal value ffff@12")),
			visible: true,
		},
		{
			key: kvrows.Key{[]byte("ffff"), 11, kvrows.ProposalKeyType},
			val: kvrows.MakeProposalValue(txk, []byte("proposal value ffff@11")),
		},
		{
			key: kvrows.Key{[]byte("ffff"), 10, kvrows.ProposalKeyType},
			val: kvrows.MakeProposalValue(txk, kvrows.MakeTombstoneValue()),
		},
		{
			key: kvrows.Key{[]byte("ffff"), 8, kvrows.DurableKeyType},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("ffff@8")}),
		},
		{
			key:     kvrows.Key{[]byte("gggg"), 858, kvrows.DurableKeyType},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("gggg@858")}),
			visible: true,
		},
		{
			key: kvrows.Key{[]byte("hhhh"), 120, kvrows.ProposalKeyType},
			val: kvrows.MakeProposalValue(txk, kvrows.MakeTombstoneValue()),
		},
		{
			key: kvrows.Key{[]byte("hhhh"), 119, kvrows.ProposalKeyType},
			val: kvrows.MakeProposalValue(txk,
				kvrows.MakeRowValue([]sql.Value{sql.StringValue("hhhh@119")})),
		},
		{
			key: kvrows.Key{[]byte("hhhh"), 1234567, kvrows.DurableKeyType},
			val: kvrows.MakeTombstoneValue(),
		},
		{
			key: kvrows.Key{[]byte("hhhh"), 123456, kvrows.DurableKeyType},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("hhhh@123456")}),
		},
		{
			key: kvrows.Key{[]byte("iiii"), 9999999, kvrows.DurableKeyType},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("iiii@9999999")}),
		},
		{
			key:     kvrows.Key{[]byte("iiii"), 8, kvrows.DurableKeyType},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("iiii@8")}),
			visible: true,
		},
	}

	initializeMap(t, st, 1000, keyVals)

	rel := relation{
		txKey: txk,
		mid:   1000,
		sid:   999999,
		state: kvrows.AbortedState,
	}
	keys, vals, _, err := lkv.ScanRelation(ctx, rel, 999999, nil, 1024, nil)
	if err != io.EOF {
		t.Errorf("ScanRelation failed with %s", err)
	}
	checkScan(t, keyVals, 0, keys, vals)
	if countVisible(keyVals) != len(keys) {
		t.Errorf("ScanRelation: got %d key-values; want %d", len(keys), countVisible(keyVals))
	}

	idx := 0
	cnt := 0
	var next []byte
	for {
		keys, vals, next, err = lkv.ScanRelation(ctx, rel, 999999, nil, 1, next)
		if err != nil && err != io.EOF {
			t.Errorf("ScanRelation failed with %s", err)
		}
		cnt += len(keys)
		idx = checkScan(t, keyVals, idx, keys, vals)
		if err == io.EOF {
			break
		}
	}
	if countVisible(keyVals) != cnt {
		t.Errorf("ScanRelation: got %d key-values; want %d", cnt, countVisible(keyVals))
	}

	txk2 := kvrows.TransactionKey{
		MID:   99999,
		Key:   []byte("abcdefghijklmn"),
		TID:   12345678,
		Epoch: 53,
	}
	keyVals2 := []keyValue{
		{
			key:     kvrows.Key{[]byte("bbbb"), 50, kvrows.DurableKeyType},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb@50")}),
			visible: true,
		},
		{
			key:     kvrows.Key{[]byte("cccc"), 12, kvrows.ProposalKeyType},
			val:     kvrows.MakeProposalValue(txk2, []byte("proposal value cccc@12")),
			visible: true,
		},
		{
			key:     kvrows.Key{[]byte("dddd"), 50, kvrows.DurableKeyType},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("dddd@50")}),
			visible: true,
		},
		{
			key: kvrows.Key{[]byte("eeee"), 13, kvrows.ProposalKeyType},
			val: kvrows.MakeProposalValue(txk2, kvrows.MakeTombstoneValue()),
		},
		{
			key: kvrows.Key{[]byte("eeee"), 12, kvrows.ProposalKeyType},
			val: kvrows.MakeProposalValue(txk2, []byte("proposal value eeee@12")),
		},
		{
			key:     kvrows.Key{[]byte("ffff"), 50, kvrows.DurableKeyType},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("ffff@50")}),
			visible: true,
		},
	}
	initializeMap(t, st, 2000, keyVals2)

	rel = relation{
		txKey: txk,
		mid:   2000,
		sid:   999999,
		state: kvrows.CommittedState,
	}
	keys, vals, _, err = lkv.ScanRelation(ctx, rel, 999999, nil, 1024, nil)
	if err != io.EOF {
		t.Errorf("ScanRelation failed with %s", err)
	}
	checkScan(t, keyVals2, 0, keys, vals)
	if countVisible(keyVals2) != len(keys) {
		t.Errorf("ScanRelation: got %d key-values; want %d", len(keys), countVisible(keyVals2))
	}

	rel = relation{
		txKey: txk,
		mid:   2000,
		sid:   999999,
		state: kvrows.ActiveState,
	}
	keys, vals, next, err = lkv.ScanRelation(ctx, rel, 999999, nil, 1024, nil)
	bperr, ok := err.(*kvrows.ErrBlockingProposal)
	if !ok {
		t.Errorf("ScanRelation: got %s; want blocking proposals error", err)
	} else if !bperr.TxKey.Equal(txk2) {
		t.Errorf("ScanRelation: got key %v; want %v", bperr, txk2)
	}
	if len(keys) != 1 {
		t.Errorf("ScanRelation: got %d keys; want 1", len(keys))
	}

	idx = checkScan(t, keyVals2, 0, keys, vals)
	cnt = len(keys)

	rel = relation{
		txKey: txk,
		mid:   2000,
		sid:   999999,
		state: kvrows.CommittedState,
	}
	keys, vals, _, err = lkv.ScanRelation(ctx, rel, 999999, nil, 1024, next)
	if err != io.EOF {
		t.Errorf("ScanRelation failed with %s", err)
	}
	checkScan(t, keyVals2, idx, keys, vals)
	cnt += len(keys)
	if countVisible(keyVals2) != cnt {
		t.Errorf("ScanRelation: got %d key-values; want %d", cnt, countVisible(keyVals2))
	}
}

func checkRelation(t *testing.T, ctx context.Context, st kvrows.Store, rel kvrows.Relation,
	wantKeys, wantVals [][]byte) {

	keys, vals, _, err := st.ScanRelation(ctx, rel, kvrows.MaximumVersion, nil, 1024, nil)
	if err != io.EOF {
		t.Errorf("ScanRelation failed with %s", err)
	}
	if len(wantKeys) != len(keys) {
		t.Errorf("ScanRelation: got %d keys; want %d", len(keys), len(wantKeys))
	}
	for i := range keys {
		if !bytes.Equal(keys[i].Key, wantKeys[i]) {
			t.Errorf("ScanRelation: got %v key at %d; want %v", keys[i].Key, i, wantKeys[i])
		}
	}
	if !testutil.DeepEqual(vals, wantVals) {
		t.Errorf("ScanRelation: got %v keys; want %v", vals, wantVals)
	}
}

func testInsertRelation(t *testing.T, st kvrows.Store) {
	ctx := context.Background()

	txk := kvrows.TransactionKey{
		MID:   10000,
		Key:   []byte("abcdefghijklmn"),
		TID:   99,
		Epoch: 888,
	}

	rel := relation{
		txKey: txk,
		mid:   10000,
		sid:   10,
		state: kvrows.AbortedState,
	}

	err := st.InsertRelation(ctx, rel,
		[][]byte{
			[]byte("bbbb key"),
			[]byte("cccc key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
		})
	if err != nil {
		t.Errorf("InsertRelation: failed with %s", err)
	}

	rel.sid += 1
	checkRelation(t, ctx, st, rel,
		[][]byte{
			[]byte("bbbb key"),
			[]byte("cccc key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
		})

	rel.sid += 1
	err = st.InsertRelation(ctx, rel,
		[][]byte{
			[]byte("bbbb key"),
			[]byte("cccc key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
		})
	if err == nil {
		t.Errorf("InsertRelation: did not fail")
	}

	rel.sid += 1
	err = st.InsertRelation(ctx, rel,
		[][]byte{
			[]byte("aaaa key"),
			[]byte("dddd key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("dddd row")}),
		})
	if err != nil {
		t.Errorf("InsertRelation: failed with %s", err)
	}

	rel.sid += 1
	checkRelation(t, ctx, st, rel,
		[][]byte{
			[]byte("aaaa key"),
			[]byte("bbbb key"),
			[]byte("cccc key"),
			[]byte("dddd key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("dddd row")}),
		})

	rel.sid += 1
	err = st.InsertRelation(ctx, rel,
		[][]byte{
			[]byte("eeee key"),
			[]byte("eeee key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row #1")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row #2")}),
		})
	if err == nil {
		t.Errorf("InsertRelation did not fail")
	}

	rel.sid += 1
	checkRelation(t, ctx, st, rel,
		[][]byte{
			[]byte("aaaa key"),
			[]byte("bbbb key"),
			[]byte("cccc key"),
			[]byte("dddd key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("dddd row")}),
		})
}

func testDeleteRelation(t *testing.T, st kvrows.Store) {
	ctx := context.Background()

	txk := kvrows.TransactionKey{
		MID:   20000,
		Key:   []byte("abcdefghijklmn"),
		TID:   99,
		Epoch: 888,
	}

	rel := relation{
		txKey: txk,
		mid:   20000,
		sid:   10,
		state: kvrows.AbortedState,
	}

	err := st.InsertRelation(ctx, rel,
		[][]byte{
			[]byte("aaaa key"),
			[]byte("bbbb key"),
			[]byte("cccc key"),
			[]byte("dddd key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("dddd row")}),
		})
	if err != nil {
		t.Errorf("InsertRelation: failed with %s", err)
	}

	rel.sid += 1
	checkRelation(t, ctx, st, rel,
		[][]byte{
			[]byte("aaaa key"),
			[]byte("bbbb key"),
			[]byte("cccc key"),
			[]byte("dddd key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("dddd row")}),
		})

	rel.sid += 1
	err = st.DeleteRelation(ctx, rel,
		[]kvrows.Key{
			kvrows.Key{[]byte("bbbb key"), 10, kvrows.ProposalKeyType},
		})
	if err != nil {
		t.Errorf("DeleteRelation failed with %s", err)
	}

	rel.sid += 1
	checkRelation(t, ctx, st, rel,
		[][]byte{
			[]byte("aaaa key"),
			[]byte("cccc key"),
			[]byte("dddd key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("dddd row")}),
		})

	rel.sid += 1
	err = st.DeleteRelation(ctx, rel,
		[]kvrows.Key{
			kvrows.Key{[]byte("bbbb key"), 10, kvrows.ProposalKeyType},
		})
	if err == nil {
		t.Errorf("DeleteRelation did not fail")
	}

	rel.sid += 1
	err = st.InsertRelation(ctx, rel,
		[][]byte{
			[]byte("bbbb key"),
			[]byte("eeee key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row")}),
		})
	if err != nil {
		t.Errorf("InsertRelation: failed with %s", err)
	}

	rel.sid += 1
	checkRelation(t, ctx, st, rel,
		[][]byte{
			[]byte("aaaa key"),
			[]byte("bbbb key"),
			[]byte("cccc key"),
			[]byte("dddd key"),
			[]byte("eeee key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("dddd row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row")}),
		})

	rel.sid += 1
	err = st.DeleteRelation(ctx, rel,
		[]kvrows.Key{
			kvrows.Key{[]byte("bbbb key"), 15, kvrows.ProposalKeyType},
			kvrows.Key{[]byte("dddd key"), 10, kvrows.ProposalKeyType},
		})
	if err != nil {
		t.Errorf("DeleteRelation failed with %s", err)
	}

	rel.sid += 1
	checkRelation(t, ctx, st, rel,
		[][]byte{
			[]byte("aaaa key"),
			[]byte("cccc key"),
			[]byte("eeee key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row")}),
		})
}

func TestBadger(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

	st, err := badger.OpenStore(filepath.Join("testdata", "teststore"))
	if err != nil {
		t.Fatal(err)
	}
	testReadWriteList(t, localkv.NewStore(st))
	testScanRelation(t, st)
	testInsertRelation(t, localkv.NewStore(st))
	testDeleteRelation(t, localkv.NewStore(st))
}

func TestBBolt(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

	st, err := bbolt.OpenStore(filepath.Join("testdata", "teststore"))
	if err != nil {
		t.Fatal(err)
	}
	testReadWriteList(t, localkv.NewStore(st))
	testScanRelation(t, st)
	testInsertRelation(t, localkv.NewStore(st))
	testDeleteRelation(t, localkv.NewStore(st))
}
