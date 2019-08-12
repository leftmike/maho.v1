package test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/sql"
)

const (
	cmdVersionedGet = iota
	cmdVersionedSet
)

type versionedCmd struct {
	cmd  int
	key  string
	ver  uint64
	val  []byte
	fail bool
}

func testVersionedTable(t *testing.T, st kvrows.Store, mid uint64, cmds []versionedCmd) {
	vtbl := kvrows.MakeVersionedTable(st, mid)

	for _, cmd := range cmds {
		switch cmd.cmd {
		case cmdVersionedGet:
			ver, err := vtbl.Get(sql.StringValue(cmd.key),
				func(val []byte) error {
					if bytes.Compare(val, cmd.val) != 0 {
						return fmt.Errorf("got value %v want %v", val, cmd.val)
					}
					return nil
				})
			if cmd.fail {
				if err == nil {
					t.Errorf("Get(%s) did not fail", cmd.key)
				}
			} else if err != nil {
				t.Errorf("Get(%s) failed with %s", cmd.key, err)
			} else if ver != cmd.ver {
				t.Errorf("Get(%s) got version %d want %d", cmd.key, ver, cmd.ver)
			}
		case cmdVersionedSet:
			err := vtbl.Set(sql.StringValue(cmd.key), cmd.ver, cmd.val)
			if cmd.fail {
				if err == nil {
					t.Errorf("Set(%s, %d) did not fail", cmd.key, cmd.ver)
				}
			} else if err != nil {
				t.Errorf("Set(abcxyz) failed with %s", err)
			}
		}
	}
}

func RunKVRowsTest(t *testing.T, st kvrows.Store) {
	testVersionedTable(t, st, 1,
		[]versionedCmd{
			{cmd: cmdVersionedGet, key: "abcxyz", fail: true},
			{cmd: cmdVersionedSet, key: "abcxyz", val: []byte{1, 2, 3, 4, 5}},
			{cmd: cmdVersionedGet, key: "abcxyz", ver: 1, val: []byte{1, 2, 3, 4, 5}},
		})
}
