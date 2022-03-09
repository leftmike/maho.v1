package kvrows_test

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/leftmike/maho/storage/encode"
	"github.com/leftmike/maho/storage/kvrows"
	"github.com/leftmike/maho/testutil"
)

const (
	iterateCmd = iota
	updaterCmd
	updateCmd
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
	oldVal  string
	newVal  string
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
			it, err := kv.Iterate([]byte(cmd.key), encode.MaxKey)
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

		case updaterCmd:
			if updater != nil {
				panic("updater: updater is not nil")
			}

			var err error
			updater, err = kv.Updater()
			if err != nil {
				t.Errorf("%sUpdater() failed with %s", cmd.fln, err)
			}

		case updateCmd:
			if updater == nil {
				panic("update: updater is nil")
			}

			err := updater.Update([]byte(cmd.key),
				func(val []byte) ([]byte, error) {
					if string(val) != cmd.oldVal {
						return nil, fmt.Errorf("val: got %s want %s", string(val), cmd.oldVal)
					}
					return []byte(cmd.newVal), nil
				})
			if cmd.fail {
				if err == nil {
					t.Errorf("%sUpdate() did not fail", cmd.fln)
				}
			} else if err != nil {
				t.Errorf("%sUpdate() failed with %s", cmd.fln, err)
			}

		case commitCmd:
			if updater == nil {
				panic("commit: updater is nil")
			}
			err := updater.Commit(true)
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
			{fln: fln(), cmd: updaterCmd},
			{fln: fln(), cmd: updateCmd, key: "Aaaa", newVal: "aaa@2"},
			{fln: fln(), cmd: updateCmd, key: "Accc", newVal: "ccc@2"},
			{fln: fln(), cmd: updateCmd, key: "Abbb", newVal: "bbb@2"},
			{fln: fln(), cmd: commitCmd},

			{fln: fln(), cmd: iterateCmd, key: "A",
				keyVals: []keyVal{
					{"Aaaa", "aaa@2"},
					{"Abbb", "bbb@2"},
					{"Accc", "ccc@2"},
				},
			},

			{fln: fln(), cmd: updaterCmd},
			{fln: fln(), cmd: updateCmd, key: "Abbb", oldVal: "bbb@2", newVal: "bbb@3"},
			{fln: fln(), cmd: updateCmd, key: "Addd", newVal: "ddd@3"},
			{fln: fln(), cmd: commitCmd},

			{fln: fln(), cmd: iterateCmd, key: "A",
				keyVals: []keyVal{
					{"Aaaa", "aaa@2"},
					{"Abbb", "bbb@3"},
					{"Accc", "ccc@2"},
					{"Addd", "ddd@3"},
				},
			},

			{fln: fln(), cmd: updaterCmd},
			{fln: fln(), cmd: updateCmd, key: "Abbb", oldVal: "bbb@3", newVal: "bbb@4"},
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

func TestBadgerKVStore(t *testing.T) {
	err := testutil.CleanDir("testdata", []string{".gitignore"})
	if err != nil {
		t.Fatal(err)
	}

	kv, err := kvrows.MakeBadgerKV("testdata",
		testutil.SetupLogger(filepath.Join("testdata", "badger_kv.log")))
	if err != nil {
		t.Fatal(err)
	}

	testKV(t, kv)
}
