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

func (_ relation) AbortedTransaction(txKey kvrows.TransactionKey) bool {
	return true
}

func testScanRelation(t *testing.T, st localkv.Store) {
	t.Helper()

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

	keys, vals, _, err := lkv.ScanRelation(ctx, relation{txKey: txk, mid: 1000, sid: 999999},
		999999, nil, 1024, nil)
	if err != io.EOF {
		t.Errorf("ScanRelation failed with %s", err)
	}
	checkScan(t, keyVals, 0, keys, vals)

	idx := 0
	var next interface{}
	for {
		keys, vals, next, err = lkv.ScanRelation(ctx, relation{txKey: txk, mid: 1000, sid: 999999},
			999999, nil, 1, next)
		if err != nil && err != io.EOF {
			t.Errorf("ScanRelation failed with %s", err)
		}
		idx = checkScan(t, keyVals, idx, keys, vals)
		if err == io.EOF {
			break
		}
	}
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
}
