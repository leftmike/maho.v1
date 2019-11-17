package localkv_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/engine/badger"
	"github.com/leftmike/maho/engine/bbolt"
	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/localkv"
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
	for i, cmd := range cmds {
		switch cmd.cmd {
		case cmdReadValue:
			ver, val, err := st.ReadValue(mid, cmd.key)
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
			keys, vals, err := st.ListValues(mid)
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
			err := st.WriteValue(mid, cmd.key, cmd.ver, cmd.val)
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

func testReadWrite(t *testing.T, st kvrows.Store) {
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

func TestBadger(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatalf("CleanDir() failed with %s", err)
	}

	st, err := badger.OpenStore(filepath.Join("testdata", "teststore"))
	if err != nil {
		t.Fatal(err)
	}
	testReadWrite(t, localkv.NewStore(st))
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
	testReadWrite(t, localkv.NewStore(st))
}
