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
				t.Errorf("Set(%s, %d) failed with %s", cmd.key, cmd.ver, err)
			}
		}
	}
}

func RunKVRowsTest(t *testing.T, st kvrows.Store) {
	versionedCmds := []versionedCmd{
		{cmd: cmdVersionedGet, key: "abcxyz", fail: true},
		{cmd: cmdVersionedSet, key: "abcxyz", val: []byte{1, 2, 3, 4, 5}},
		{cmd: cmdVersionedGet, key: "abcxyz", ver: 1, val: []byte{1, 2, 3, 4, 5}},
		{cmd: cmdVersionedGet, key: "abc", fail: true},
		{cmd: cmdVersionedGet, key: "xyz", fail: true},
		{cmd: cmdVersionedGet, key: "abcxyz0", fail: true},
		{cmd: cmdVersionedSet, key: "abcxyz", ver: 0, val: []byte{1}, fail: true},
		{cmd: cmdVersionedSet, key: "abcxyz", ver: 2, val: []byte{1}, fail: true},

		{cmd: cmdVersionedSet, key: "abc0", val: []byte{1, 0}},
		{cmd: cmdVersionedSet, key: "abc1", val: []byte{1, 1}},
		{cmd: cmdVersionedSet, key: "abc2", val: []byte{1, 2}},
		{cmd: cmdVersionedSet, key: "abc3", val: []byte{1, 3}},
		{cmd: cmdVersionedSet, key: "abc4", val: []byte{1, 4}},
		{cmd: cmdVersionedSet, key: "abc5", val: []byte{1, 5}},

		{cmd: cmdVersionedGet, key: "abc0", ver: 1, val: []byte{1, 0}},
		{cmd: cmdVersionedGet, key: "abc1", ver: 1, val: []byte{1, 1}},
		{cmd: cmdVersionedGet, key: "abc2", ver: 1, val: []byte{1, 2}},
		{cmd: cmdVersionedGet, key: "abc3", ver: 1, val: []byte{1, 3}},
		{cmd: cmdVersionedGet, key: "abc4", ver: 1, val: []byte{1, 4}},
		{cmd: cmdVersionedGet, key: "abc5", ver: 1, val: []byte{1, 5}},

		{cmd: cmdVersionedSet, key: "abc1", ver: 1, val: []byte{2, 1}},
		{cmd: cmdVersionedSet, key: "abc3", ver: 1, val: []byte{2, 3}},
		{cmd: cmdVersionedSet, key: "abc4", ver: 1, val: []byte{2, 4}},

		{cmd: cmdVersionedGet, key: "abc0", ver: 1, val: []byte{1, 0}},
		{cmd: cmdVersionedGet, key: "abc1", ver: 2, val: []byte{2, 1}},
		{cmd: cmdVersionedGet, key: "abc2", ver: 1, val: []byte{1, 2}},
		{cmd: cmdVersionedGet, key: "abc3", ver: 2, val: []byte{2, 3}},
		{cmd: cmdVersionedGet, key: "abc4", ver: 2, val: []byte{2, 4}},
		{cmd: cmdVersionedGet, key: "abc5", ver: 1, val: []byte{1, 5}},

		{cmd: cmdVersionedSet, key: "abc4", ver: 2, val: []byte{3, 4}},

		{cmd: cmdVersionedGet, key: "abc0", ver: 1, val: []byte{1, 0}},
		{cmd: cmdVersionedGet, key: "abc1", ver: 2, val: []byte{2, 1}},
		{cmd: cmdVersionedGet, key: "abc2", ver: 1, val: []byte{1, 2}},
		{cmd: cmdVersionedGet, key: "abc3", ver: 2, val: []byte{2, 3}},
		{cmd: cmdVersionedGet, key: "abc4", ver: 3, val: []byte{3, 4}},
		{cmd: cmdVersionedGet, key: "abc5", ver: 1, val: []byte{1, 5}},
	}

	testVersionedTable(t, st, 1, versionedCmds)
	testVersionedTable(t, st, 2, versionedCmds)
	testVersionedTable(t, st, 1919191, versionedCmds)
}
