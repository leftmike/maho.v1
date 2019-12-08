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
			key:  kvrows.Key{SQLKey: []byte("abcd0")},
			fail: true,
		},
		{
			cmd: cmdListValues,
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{SQLKey: []byte("abcd0")},
			ver: 1,
			val: []byte("1234567890"),
		},
		{
			cmd: cmdReadValue,
			key: kvrows.Key{SQLKey: []byte("abcd0")},
			ver: 1,
			val: []byte("1234567890"),
		},
		{
			cmd: cmdListValues,
			keys: []kvrows.Key{
				{SQLKey: []byte("abcd0"), Version: 1},
			},
			vals: [][]byte{
				[]byte("1234567890"),
			},
		},
		{
			cmd:  cmdWriteValue,
			key:  kvrows.Key{SQLKey: []byte("abcd0")},
			ver:  2,
			val:  []byte("0987654321"),
			fail: true,
		},
		{
			cmd:  cmdWriteValue,
			key:  kvrows.Key{SQLKey: []byte("xxxyyyzzz"), Version: 10},
			ver:  11,
			val:  []byte("0987654321"),
			fail: true,
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{SQLKey: []byte("abcd0"), Version: 1},
			ver: 2,
			val: []byte("0987654321"),
		},
		{
			cmd: cmdReadValue,
			key: kvrows.Key{SQLKey: []byte("abcd0")},
			ver: 2,
			val: []byte("0987654321"),
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{SQLKey: []byte("abcd0"), Version: 2},
			ver: 10,
			val: []byte("1234567890987654321"),
		},
		{
			cmd:  cmdWriteValue,
			key:  kvrows.Key{SQLKey: []byte("abcd0"), Version: 10},
			ver:  5,
			val:  []byte("0000000000"),
			fail: true,
		},
		{
			cmd: cmdReadValue,
			key: kvrows.Key{SQLKey: []byte("abcd0")},
			ver: 10,
			val: []byte("1234567890987654321"),
		},
		{
			cmd: cmdListValues,
			keys: []kvrows.Key{
				{SQLKey: []byte("abcd0"), Version: 10},
			},
			vals: [][]byte{
				[]byte("1234567890987654321"),
			},
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{SQLKey: []byte("abcd1")},
			ver: 1,
			val: []byte("one"),
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{SQLKey: []byte("abcd2")},
			ver: 2,
			val: []byte("two"),
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{SQLKey: []byte("abcd3")},
			ver: 3,
			val: []byte("three"),
		},
		{
			cmd: cmdWriteValue,
			key: kvrows.Key{SQLKey: []byte("abcd4")},
			ver: 4,
			val: []byte("four"),
		},
		{
			cmd: cmdListValues,
			keys: []kvrows.Key{
				{SQLKey: []byte("abcd0"), Version: 10},
				{SQLKey: []byte("abcd1"), Version: 1},
				{SQLKey: []byte("abcd2"), Version: 2},
				{SQLKey: []byte("abcd3"), Version: 3},
				{SQLKey: []byte("abcd4"), Version: 4},
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
	scanVal []byte // If the key is a proposal.
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
		if keyVals[idx].key.Version == kvrows.ProposalVersion {
			if !bytes.Equal(vals[jdx], keyVals[idx].scanVal) {
				t.Errorf("ScanRelation: got %v value; wanted %v", vals[jdx], keyVals[idx].scanVal)
			}
		} else if !bytes.Equal(vals[jdx], keyVals[idx].val) {
			t.Errorf("ScanRelation: got %v value; wanted %v", vals[jdx], keyVals[idx].val)
		}
		idx += 1
	}

	return idx
}

func getAbortedState(tid kvrows.TransactionID) (kvrows.TransactionState, uint64) {
	return kvrows.AbortedState, 0
}

func getCommittedState(tid kvrows.TransactionID) (kvrows.TransactionState, uint64) {
	return kvrows.CommittedState, 100000
}

func getActiveState(tid kvrows.TransactionID) (kvrows.TransactionState, uint64) {
	return kvrows.ActiveState, 0
}

func testScanRelation(t *testing.T, st localkv.Store) {
	ctx := context.Background()
	lkv := localkv.NewStore(st)

	tid := kvrows.TransactionID{
		Node:    123,
		Epoch:   888,
		LocalID: 99,
	}
	keyVals := []keyValue{
		{
			key:     kvrows.Key{[]byte("bbbb"), 50},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb@50")}),
			visible: true,
		},
		{
			key: kvrows.Key{[]byte("bbbb"), 49},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb@49")}),
		},
		{
			key: kvrows.Key{[]byte("bbbb"), 48},
			val: kvrows.MakeTombstoneValue(),
		},
		{
			key: kvrows.Key{[]byte("bbbb"), 47},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb@47")}),
		},
		{
			key:     kvrows.Key{[]byte("dddd"), 100},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("dddd@100")}),
			visible: true,
		},
		{
			key: kvrows.Key{[]byte("eeee"), 10},
			val: kvrows.MakeTombstoneValue(),
		},
		{
			key: kvrows.Key{[]byte("eeee"), 9},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee@9")}),
		},
		{
			key: kvrows.Key{[]byte("eeee"), 8},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee@8")}),
		},
		{
			key:     kvrows.Key{[]byte("ffff"), kvrows.ProposalVersion},
			visible: true,
			val: kvrows.MakeProposalValue(tid,
				[]kvrows.Proposal{
					{10, kvrows.MakeTombstoneValue()},
					{11, []byte("proposal value ffff@11")},
					{12, []byte("proposal value ffff@12")},
				}),
			scanVal: []byte("proposal value ffff@12"),
		},
		{
			key: kvrows.Key{[]byte("ffff"), 8},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("ffff@8")}),
		},
		{
			key:     kvrows.Key{[]byte("gggg"), 858},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("gggg@858")}),
			visible: true,
		},

		{
			key: kvrows.Key{[]byte("hhhh"), kvrows.ProposalVersion},
			val: kvrows.MakeProposalValue(tid,
				[]kvrows.Proposal{
					{119, kvrows.MakeRowValue([]sql.Value{sql.StringValue("hhhh@119")})},
					{120, kvrows.MakeTombstoneValue()},
				}),
		},
		{
			key: kvrows.Key{[]byte("hhhh"), 1234567},
			val: kvrows.MakeTombstoneValue(),
		},
		{
			key: kvrows.Key{[]byte("hhhh"), 123456},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("hhhh@123456")}),
		},
		{
			key: kvrows.Key{[]byte("iiii"), 9999999},
			val: kvrows.MakeRowValue([]sql.Value{sql.StringValue("iiii@9999999")}),
		},
		{
			key:     kvrows.Key{[]byte("iiii"), 8},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("iiii@8")}),
			visible: true,
		},
	}

	initializeMap(t, st, 1000, keyVals)

	keys, vals, _, err := lkv.ScanRelation(ctx, getAbortedState, tid, 200, 1000, 999999, 88888,
		nil)
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
		keys, vals, next, err = lkv.ScanRelation(ctx, getAbortedState, tid, 200, 1000, 999999,
			1, next)
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

	tid2 := kvrows.TransactionID{
		Node:    10,
		Epoch:   53,
		LocalID: 12345678,
	}
	keyVals2 := []keyValue{
		{
			key:     kvrows.Key{[]byte("bbbb"), 50},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb@50")}),
			visible: true,
		},
		{
			key: kvrows.Key{[]byte("cccc"), kvrows.ProposalVersion},
			val: kvrows.MakeProposalValue(tid2,
				[]kvrows.Proposal{
					{SID: 12, Value: []byte("proposal value cccc@12")},
				}),
			scanVal: []byte("proposal value cccc@12"),
			visible: true,
		},
		{
			key:     kvrows.Key{[]byte("dddd"), 50},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("dddd@50")}),
			visible: true,
		},
		{
			key: kvrows.Key{[]byte("eeee"), kvrows.ProposalVersion},
			val: kvrows.MakeProposalValue(tid2,
				[]kvrows.Proposal{
					{12, []byte("proposal value eeee@12")},
					{13, kvrows.MakeTombstoneValue()},
				}),
		},
		{
			key:     kvrows.Key{[]byte("ffff"), 50},
			val:     kvrows.MakeRowValue([]sql.Value{sql.StringValue("ffff@50")}),
			visible: true,
		},
	}
	initializeMap(t, st, 2000, keyVals2)

	keys, vals, _, err = lkv.ScanRelation(ctx, getCommittedState, tid, 999999, 2000, 999999, 1024,
		nil)
	if err != io.EOF {
		t.Errorf("ScanRelation failed with %s", err)
	}
	checkScan(t, keyVals2, 0, keys, vals)
	if countVisible(keyVals2) != len(keys) {
		t.Errorf("ScanRelation: got %d key-values; want %d", len(keys), countVisible(keyVals2))
	}

	keys, vals, next, err = lkv.ScanRelation(ctx, getActiveState, tid, 999999, 2000, 999999, 1024,
		nil)
	bperr, ok := err.(*kvrows.ErrBlockingProposal)
	if !ok {
		t.Errorf("ScanRelation: got %s; want blocking proposals error", err)
	} else if bperr.TID != tid2 {
		t.Errorf("ScanRelation: got key %v; want %v", bperr, tid2)
	}
	if len(keys) != 1 {
		t.Errorf("ScanRelation: got %d keys; want 1", len(keys))
	}

	idx = checkScan(t, keyVals2, 0, keys, vals)
	cnt = len(keys)

	keys, vals, _, err = lkv.ScanRelation(ctx, getCommittedState, tid, 999999, 2000, 999999, 1024,
		next)
	if err != io.EOF {
		t.Errorf("ScanRelation failed with %s", err)
	}
	checkScan(t, keyVals2, idx, keys, vals)
	cnt += len(keys)
	if countVisible(keyVals2) != cnt {
		t.Errorf("ScanRelation: got %d key-values; want %d", cnt, countVisible(keyVals2))
	}
}

func checkRelation(t *testing.T, ctx context.Context, st kvrows.Store, getState kvrows.GetTxState,
	tid kvrows.TransactionID, sid, mid uint64, wantKeys, wantVals [][]byte) {

	keys, vals, _, err := st.ScanRelation(ctx, getState, tid, sid, mid, kvrows.MaximumVersion,
		1024, nil)
	if err != io.EOF {
		t.Errorf("ScanRelation failed with %s", err)
	}
	if len(wantKeys) != len(keys) {
		t.Errorf("ScanRelation: got %d keys; want %d", len(keys), len(wantKeys))
	}
	for i := range keys {
		if !bytes.Equal(keys[i].SQLKey, wantKeys[i]) {
			t.Errorf("ScanRelation: got %v key at %d; want %v", keys[i].SQLKey, i, wantKeys[i])
		}
	}
	if !testutil.DeepEqual(vals, wantVals) {
		t.Errorf("ScanRelation: got %v keys; want %v", vals, wantVals)
	}
}

func testInsertRelation(t *testing.T, st kvrows.Store) {
	ctx := context.Background()

	tid := kvrows.TransactionID{
		Node:    10,
		Epoch:   888,
		LocalID: 99,
	}

	sid := uint64(10)
	err := st.InsertRelation(ctx, getAbortedState, tid, sid, 10000,
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

	sid += 1
	checkRelation(t, ctx, st, getAbortedState, tid, sid, 10000,
		[][]byte{
			[]byte("bbbb key"),
			[]byte("cccc key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
		})

	sid += 1
	err = st.InsertRelation(ctx, getAbortedState, tid, sid, 10000,
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

	sid += 1
	err = st.InsertRelation(ctx, getAbortedState, tid, sid, 10000,
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

	sid += 1
	checkRelation(t, ctx, st, getAbortedState, tid, sid, 10000,
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

	sid += 1
	err = st.InsertRelation(ctx, getAbortedState, tid, sid, 10000,
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

	sid += 1
	checkRelation(t, ctx, st, getAbortedState, tid, sid, 10000,
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

	tid2 := kvrows.TransactionID{
		Node:    10,
		Epoch:   888,
		LocalID: 1001,
	}

	sid2 := uint64(100)
	err = st.InsertRelation(ctx, getAbortedState, tid2, sid2, 10000,
		[][]byte{
			[]byte("eeee key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row@2")}),
		})
	if err != nil {
		t.Errorf("InsertRelation: failed with %s", err)
	}

	sid += 1
	err = st.InsertRelation(ctx, getAbortedState, tid, sid, 10000,
		[][]byte{
			[]byte("eeee key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row@1")}),
		})
	if err != nil {
		t.Errorf("InsertRelation: failed with %s", err)
	}

	sid += 1
	checkRelation(t, ctx, st, getAbortedState, tid, sid, 10000,
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
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row@1")}),
		})

	sid2 += 1
	err = st.InsertRelation(ctx, getCommittedState, tid2, sid2, 10000,
		[][]byte{
			[]byte("eeee key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row@2")}),
		})
	if err == nil {
		t.Errorf("InsertRelation did not fail")
	}

	sid2 += 1
	err = st.InsertRelation(ctx, getActiveState, tid2, sid2, 10000,
		[][]byte{
			[]byte("eeee key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row@2")}),
		})
	if err == nil {
		t.Errorf("InsertRelation did not fail")
	}
	bperr, ok := err.(*kvrows.ErrBlockingProposal)
	if !ok {
		t.Errorf("InsertRelation: got %s; want blocking proposals error", err)
	} else if bperr.TID != tid {
		t.Errorf("InsertRelation: got key %v; want %v", bperr, tid)
	}
}

func testModifyRelation(t *testing.T, st kvrows.Store) {
	ctx := context.Background()

	tid := kvrows.TransactionID{
		Node:    20,
		Epoch:   888,
		LocalID: 99,
	}

	sid := uint64(10)
	err := st.InsertRelation(ctx, getAbortedState, tid, sid, 20000,
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

	sid += 1
	checkRelation(t, ctx, st, getAbortedState, tid, sid, 20000,
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

	sid += 1
	err = st.ModifyRelation(ctx, getAbortedState, tid, sid, 20000,
		[]kvrows.Key{
			kvrows.Key{[]byte("bbbb key"), kvrows.ProposalVersion},
		}, nil)
	if err != nil {
		t.Errorf("ModifyRelation failed with %s", err)
	}

	sid += 1
	checkRelation(t, ctx, st, getAbortedState, tid, sid, 20000,
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

	sid += 1
	err = st.ModifyRelation(ctx, getAbortedState, tid, sid, 20000,
		[]kvrows.Key{
			kvrows.Key{[]byte("bbbb key"), kvrows.ProposalVersion},
		}, nil)
	if err == nil {
		t.Errorf("ModifyRelation did not fail")
	}

	sid += 1
	err = st.ModifyRelation(ctx, getAbortedState, tid, sid, 20000,
		[]kvrows.Key{
			kvrows.Key{[]byte("xxxxyyyyzzzz key"), kvrows.ProposalVersion},
		}, nil)
	if err == nil {
		t.Errorf("ModifyRelation did not fail")
	}

	sid += 1
	err = st.InsertRelation(ctx, getAbortedState, tid, sid, 20000,
		[][]byte{
			[]byte("bbbb key"),
			[]byte("eeee key"),
			[]byte("ffff key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("ffff row")}),
		})
	if err != nil {
		t.Errorf("InsertRelation: failed with %s", err)
	}

	sid += 1
	checkRelation(t, ctx, st, getAbortedState, tid, sid, 20000,
		[][]byte{
			[]byte("aaaa key"),
			[]byte("bbbb key"),
			[]byte("cccc key"),
			[]byte("dddd key"),
			[]byte("eeee key"),
			[]byte("ffff key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("bbbb row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("dddd row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("ffff row")}),
		})

	sid += 1
	err = st.ModifyRelation(ctx, getAbortedState, tid, sid, 20000,
		[]kvrows.Key{
			kvrows.Key{[]byte("bbbb key"), kvrows.ProposalVersion},
			kvrows.Key{[]byte("dddd key"), kvrows.ProposalVersion},
		}, nil)
	if err != nil {
		t.Errorf("ModifyRelation failed with %s", err)
	}

	err = st.ModifyRelation(ctx, getAbortedState, tid, sid, 20000,
		[]kvrows.Key{
			kvrows.Key{[]byte("bbbb key"), kvrows.ProposalVersion},
		}, nil)
	if err == nil {
		t.Errorf("ModifyRelation did not fail")
	}

	sid += 1
	checkRelation(t, ctx, st, getAbortedState, tid, sid, 20000,
		[][]byte{
			[]byte("aaaa key"),
			[]byte("cccc key"),
			[]byte("eeee key"),
			[]byte("ffff key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("ffff row")}),
		})

	sid += 1
	err = st.ModifyRelation(ctx, getAbortedState, tid, sid, 20000,
		[]kvrows.Key{
			kvrows.Key{[]byte("ffff key"), kvrows.ProposalVersion},
			kvrows.Key{[]byte("aaaa key"), kvrows.ProposalVersion},
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("ffff row #2")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row #2")}),
		})
	if err != nil {
		t.Errorf("ModifyRelation failed with %s", err)
	}

	sid += 1
	checkRelation(t, ctx, st, getAbortedState, tid, sid, 20000,
		[][]byte{
			[]byte("aaaa key"),
			[]byte("cccc key"),
			[]byte("eeee key"),
			[]byte("ffff key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row #2")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("ffff row #2")}),
		})

	sid += 1
	err = st.ModifyRelation(ctx, getAbortedState, tid, sid, 20000,
		[]kvrows.Key{
			kvrows.Key{[]byte("cccc key"), kvrows.ProposalVersion},
			kvrows.Key{[]byte("ffff key"), kvrows.ProposalVersion},
			kvrows.Key{[]byte("aaaa key"), kvrows.ProposalVersion},
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row #2")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("ffff row #3")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row #3")}),
		})
	if err != nil {
		t.Errorf("ModifyRelation failed with %s", err)
	}

	sid += 1
	checkRelation(t, ctx, st, getAbortedState, tid, sid, 20000,
		[][]byte{
			[]byte("aaaa key"),
			[]byte("cccc key"),
			[]byte("eeee key"),
			[]byte("ffff key"),
		},
		[][]byte{
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("aaaa row #3")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("cccc row #2")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("eeee row")}),
			kvrows.MakeRowValue([]sql.Value{sql.StringValue("ffff row #3")}),
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
	testModifyRelation(t, localkv.NewStore(st))
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
	testModifyRelation(t, localkv.NewStore(st))
}
