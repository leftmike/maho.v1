package keyval_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/engine/keyval"
	"github.com/leftmike/maho/testutil"
)

const (
	iterateAtCmd = iota
	updateCmd
	getCmd
	setCmd
	commitAtCmd
	rollbackCmd
)

type keyVal struct {
	key string
	val string
	ver uint64
}

type kvCmd struct {
	fln     testutil.FileLineNumber
	cmd     int
	fail    bool
	key     string
	val     string
	ver     uint64
	keyVals []keyVal
}

func fln() testutil.FileLineNumber {
	return testutil.MakeFileLineNumber()
}

func runKVTest(t *testing.T, kv keyval.KV, cmds []kvCmd) {
	t.Helper()

	var updater keyval.Updater
	for _, cmd := range cmds {
		switch cmd.cmd {
		case iterateAtCmd:
			keyVals := cmd.keyVals
			err := kv.IterateAt(cmd.ver, []byte(cmd.key),
				func(key, val []byte, ver uint64) (bool, error) {
					if len(keyVals) == 0 {
						return false, errors.New("too many key vals")
					}
					if string(key) != keyVals[0].key {
						return false, fmt.Errorf("key: got %s want %s", string(key),
							keyVals[0].key)
					}
					if string(val) != keyVals[0].val {
						return false, fmt.Errorf("val: got %s want %s", string(val),
							keyVals[0].val)
					}
					if ver != keyVals[0].ver {
						return false, fmt.Errorf("ver: got %d want %d", ver, keyVals[0].ver)
					}
					keyVals = keyVals[1:]
					return false, nil
				})
			if cmd.fail {
				if err == nil {
					t.Errorf("%sIterateAt() did not fail", cmd.fln)
				}
			} else if err != nil {
				t.Errorf("%sIterateAt() failed with %s", cmd.fln, err)
			} else if len(keyVals) > 0 {
				t.Errorf("%sIterateAt() not enough key vals: %d", cmd.fln, len(keyVals))
			}
		case updateCmd:
			updater = kv.Update(cmd.ver)
		case getCmd:
			if updater == nil {
				panic("get: updater is nil")
			}
			err := updater.Get([]byte(cmd.key),
				func(val []byte, ver uint64) error {
					if string(val) != cmd.val {
						return fmt.Errorf("val: got %s want %s", string(val), cmd.val)
					}
					if ver != cmd.ver {
						return fmt.Errorf("ver: got %d want %d", ver, cmd.ver)
					}
					return nil
				})
			if cmd.fail {
				if err == nil {
					t.Errorf("%sGet() did not fail", cmd.fln)
				}
			} else if err != nil {
				t.Errorf("%sGet() failed with %s", cmd.fln, err)
			}
		case setCmd:
			if updater == nil {
				panic("set: updater is nil")
			}
			err := updater.Set([]byte(cmd.key), []byte(cmd.val))
			if cmd.fail {
				if err == nil {
					t.Errorf("%sSet() did not fail", cmd.fln)
				}
			} else if err != nil {
				t.Errorf("%sSet() failed with %s", cmd.fln, err)
			}
		case commitAtCmd:
			if updater == nil {
				panic("commitAt: updater is nil")
			}
			err := updater.CommitAt(cmd.ver)
			if cmd.fail {
				if err == nil {
					t.Errorf("%sCommitAt() did not fail", cmd.fln)
				}
			} else if err != nil {
				t.Errorf("%sCommitAt() failed with %s", cmd.fln, err)
			}
			updater = nil
		case rollbackCmd:
			if updater == nil {
				panic("rollback: updater is nil")
			}
			updater.Rollback()
			updater = nil
		default:
			panic(fmt.Sprintf("unexpected command: %d", cmd.cmd))
		}
	}
}

func testKV(t *testing.T, kv keyval.KV) {
	t.Helper()

	runKVTest(t, kv,
		[]kvCmd{
			{fln: fln(), cmd: iterateAtCmd, ver: 1, key: "A"},
			{fln: fln(), cmd: updateCmd, ver: 1},
			{fln: fln(), cmd: getCmd, key: "Aaaa", fail: true},
			{fln: fln(), cmd: setCmd, key: "Aaaa", val: "aaa@2"},
			{fln: fln(), cmd: setCmd, key: "Accc", val: "ccc@2"},
			{fln: fln(), cmd: setCmd, key: "Abbb", val: "bbb@2"},
			{fln: fln(), cmd: commitAtCmd, ver: 2},

			{fln: fln(), cmd: iterateAtCmd, ver: 1, key: "A"},
			{fln: fln(), cmd: iterateAtCmd, ver: 2, key: "A",
				keyVals: []keyVal{
					{"Aaaa", "aaa@2", 2},
					{"Abbb", "bbb@2", 2},
					{"Accc", "ccc@2", 2},
				},
			},

			{fln: fln(), cmd: updateCmd, ver: 2},
			{fln: fln(), cmd: getCmd, key: "Abbb", val: "bbb@2", ver: 2},
			{fln: fln(), cmd: setCmd, key: "Abbb", val: "bbb@3"},
			{fln: fln(), cmd: setCmd, key: "Addd", val: "ddd@3"},
			{fln: fln(), cmd: commitAtCmd, ver: 3},

			{fln: fln(), cmd: iterateAtCmd, ver: 1, key: "A"},
			{fln: fln(), cmd: iterateAtCmd, ver: 2, key: "A",
				keyVals: []keyVal{
					{"Aaaa", "aaa@2", 2},
					{"Abbb", "bbb@2", 2},
					{"Accc", "ccc@2", 2},
				},
			},
			{fln: fln(), cmd: iterateAtCmd, ver: 3, key: "A",
				keyVals: []keyVal{
					{"Aaaa", "aaa@2", 2},
					{"Abbb", "bbb@3", 3},
					{"Accc", "ccc@2", 2},
					{"Addd", "ddd@3", 3},
				},
			},

			{fln: fln(), cmd: updateCmd, ver: 3},
			{fln: fln(), cmd: getCmd, key: "Aaaa", val: "aaa@2", ver: 2},
			{fln: fln(), cmd: getCmd, key: "Abbb", val: "bbb@3", ver: 3},
			{fln: fln(), cmd: setCmd, key: "Abbb", val: "bbb@4"},
			{fln: fln(), cmd: rollbackCmd},

			{fln: fln(), cmd: iterateAtCmd, ver: 1, key: "A"},
			{fln: fln(), cmd: iterateAtCmd, ver: 2, key: "A",
				keyVals: []keyVal{
					{"Aaaa", "aaa@2", 2},
					{"Abbb", "bbb@2", 2},
					{"Accc", "ccc@2", 2},
				},
			},
			{fln: fln(), cmd: iterateAtCmd, ver: 3, key: "A",
				keyVals: []keyVal{
					{"Aaaa", "aaa@2", 2},
					{"Abbb", "bbb@3", 3},
					{"Accc", "ccc@2", 2},
					{"Addd", "ddd@3", 3},
				},
			},
		})
}

func TestBadgerKV(t *testing.T) {
	path := filepath.Join("testdata", "badger")
	err := testutil.CleanDir(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	kv, err := keyval.MakeBadgerKV(path)
	if err != nil {
		t.Fatal(err)
	}
	testKV(t, kv)
}
