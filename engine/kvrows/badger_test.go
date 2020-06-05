package kvrows_test

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/testutil"
)

const (
	iterateCmd = iota
	getKVCmd
	updateCmd
	getCmd
	setCmd
	commitCmd
	rollbackCmd
)

type keyVal struct {
	key string
	val string
}

type kvCmd struct {
	fln     testutil.FileLineNumber
	cmd     int
	fail    bool
	key     string
	val     string
	keyVals []keyVal
}

func fln() testutil.FileLineNumber {
	return testutil.MakeFileLineNumber()
}

func runKVTest(t *testing.T, kv kvrows.KV, cmds []kvCmd) {
	t.Helper()

	var updater kvrows.Updater
	for _, cmd := range cmds {
		switch cmd.cmd {
		case iterateCmd:
			keyVals := cmd.keyVals
			it, err := kv.Iterate([]byte(cmd.key))
			if err != nil {
				t.Errorf("%sIterate() failed with %s", cmd.fln, err)
				break
			}

			for {
				err := it.Item(
					func(key, val []byte) error {
						if len(keyVals) == 0 {
							return errors.New("too many key vals")
						}
						if string(key) != keyVals[0].key {
							return fmt.Errorf("key: got %s want %s", string(key), keyVals[0].key)
						}
						if string(val) != keyVals[0].val {
							return fmt.Errorf("val: got %s want %s", string(val), keyVals[0].val)
						}
						keyVals = keyVals[1:]
						return nil
					})
				if cmd.fail {
					if err == nil {
						t.Errorf("%sIterate() did not fail", cmd.fln)
					}
					break
				} else if err != nil {
					if err != io.EOF {
						t.Errorf("%sIterate() failed with %s", cmd.fln, err)
					}
					break
				}
			}
			if len(keyVals) > 0 {
				t.Errorf("%sIterate() not enough key vals: %d", cmd.fln, len(keyVals))
			}
			it.Close()
		case getKVCmd:
			err := kv.Get([]byte(cmd.key),
				func(val []byte) error {
					if string(val) != cmd.val {
						return fmt.Errorf("val: got %s want %s", string(val), cmd.val)
					}
					return nil
				})
			if cmd.fail {
				if err == nil {
					t.Errorf("%sGetAt() did not fail", cmd.fln)
				}
			} else if err != nil {
				t.Errorf("%sGetAt() failed with %s", cmd.fln, err)
			}
		case updateCmd:
			var err error
			updater, err = kv.Update()
			if err != nil {
				t.Errorf("%sUpdate() failed with %s", cmd.fln, err)
			}
		case getCmd:
			if updater == nil {
				panic("get: updater is nil")
			}
			err := updater.Get([]byte(cmd.key),
				func(val []byte) error {
					if string(val) != cmd.val {
						return fmt.Errorf("val: got %s want %s", string(val), cmd.val)
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
		case commitCmd:
			if updater == nil {
				panic("commit: updater is nil")
			}
			err := updater.Commit()
			if cmd.fail {
				if err == nil {
					t.Errorf("%sCommit() did not fail", cmd.fln)
				}
			} else if err != nil {
				t.Errorf("%sCommit() failed with %s", cmd.fln, err)
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

func testKV(t *testing.T, kv kvrows.KV) {
	t.Helper()

	runKVTest(t, kv,
		[]kvCmd{
			{fln: fln(), cmd: iterateCmd, key: "A"},
			{fln: fln(), cmd: getKVCmd, key: "A", fail: true},
			{fln: fln(), cmd: updateCmd},
			{fln: fln(), cmd: getCmd, key: "Aaaa", fail: true},
			{fln: fln(), cmd: setCmd, key: "Aaaa", val: "aaa@2"},
			{fln: fln(), cmd: setCmd, key: "Accc", val: "ccc@2"},
			{fln: fln(), cmd: setCmd, key: "Abbb", val: "bbb@2"},
			{fln: fln(), cmd: commitCmd},

			{fln: fln(), cmd: iterateCmd, key: "A",
				keyVals: []keyVal{
					{"Aaaa", "aaa@2"},
					{"Abbb", "bbb@2"},
					{"Accc", "ccc@2"},
				},
			},
			{fln: fln(), cmd: getKVCmd, key: "Aaaa", val: "aaa@2"},

			{fln: fln(), cmd: updateCmd},
			{fln: fln(), cmd: getCmd, key: "Abbb", val: "bbb@2"},
			{fln: fln(), cmd: setCmd, key: "Abbb", val: "bbb@3"},
			{fln: fln(), cmd: setCmd, key: "Addd", val: "ddd@3"},
			{fln: fln(), cmd: commitCmd},

			{fln: fln(), cmd: getKVCmd, key: "Abbb", val: "bbb@3"},
			{fln: fln(), cmd: iterateCmd, key: "A",
				keyVals: []keyVal{
					{"Aaaa", "aaa@2"},
					{"Abbb", "bbb@3"},
					{"Accc", "ccc@2"},
					{"Addd", "ddd@3"},
				},
			},

			{fln: fln(), cmd: updateCmd},
			{fln: fln(), cmd: getCmd, key: "Aaaa", val: "aaa@2"},
			{fln: fln(), cmd: getCmd, key: "Abbb", val: "bbb@3"},
			{fln: fln(), cmd: setCmd, key: "Abbb", val: "bbb@4"},
			{fln: fln(), cmd: rollbackCmd},

			{fln: fln(), cmd: iterateCmd, key: "A",
				keyVals: []keyVal{
					{"Aaaa", "aaa@2"},
					{"Abbb", "bbb@3"},
					{"Accc", "ccc@2"},
					{"Addd", "ddd@3"},
				},
			},
		})
}

func TestBadgerKV(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	kv, err := kvrows.MakeBadgerKV("testdata")
	if err != nil {
		t.Fatal(err)
	}

	testKV(t, kv)
}
