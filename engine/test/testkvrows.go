package test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/leftmike/maho/engine"
	kvrows "github.com/leftmike/maho/engine/kvrows.old"
	"github.com/leftmike/maho/sql"
)

const (
	cmdVersionedGet = iota
	cmdVersionedSet
	cmdVersionedList
)

type keyVerVal struct {
	key string
	ver uint64
	val []byte
}

type versionedCmd struct {
	cmd  int
	key  string
	ver  uint64
	val  []byte
	fail bool
	list []keyVerVal
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
		case cmdVersionedList:
			i := 0
			err := vtbl.List(
				func(key sql.Value, ver uint64, val []byte) error {
					if i >= len(cmd.list) {
						t.Errorf("List(%d): too many results: %s %d %v", i, key, ver, val)
						return nil
					}

					if sql.Compare(key, sql.StringValue(cmd.list[i].key)) != 0 {
						t.Errorf("List(%d): got key %s want key %s", i, key, cmd.list[i].key)
					}
					if ver != cmd.list[i].ver {
						t.Errorf("List(%d): got ver %d want ver %d", i, ver, cmd.list[i].ver)
					}
					if bytes.Compare(val, cmd.list[i].val) != 0 {
						t.Errorf("List(%d): got key %v want key %v", i, val, cmd.list[i].val)
					}
					i += 1
					return nil
				})
			if err != nil {
				t.Errorf("List(%d) failed with %s", i, err)
			}
		}
	}
}

func RunVersionedTableTest(t *testing.T, st kvrows.Store) {
	versionedCmds := []versionedCmd{
		{cmd: cmdVersionedList},

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

		{cmd: cmdVersionedList,
			list: []keyVerVal{
				{key: "abc0", ver: 1, val: []byte{1, 0}},
				{key: "abc1", ver: 2, val: []byte{2, 1}},
				{key: "abc2", ver: 1, val: []byte{1, 2}},
				{key: "abc3", ver: 2, val: []byte{2, 3}},
				{key: "abc4", ver: 3, val: []byte{3, 4}},
				{key: "abc5", ver: 1, val: []byte{1, 5}},
				{key: "abcxyz", ver: 1, val: []byte{1, 2, 3, 4, 5}},
			},
		},
	}

	testVersionedTable(t, st, 1, versionedCmds)
	testVersionedTable(t, st, 2, versionedCmds)
	testVersionedTable(t, st, 1919191, versionedCmds)
}

const (
	cmdVersionedNextStmt = iota
	cmdVersionedInsert
)

type transactedCmd struct {
	cmd  int
	row  []sql.Value
	fail bool
}

func getTx(tid uint32) (kvrows.TransactionState, uint64, error) {
	return kvrows.AbortedTransaction, 0, nil
}

func testTransactedTable(t *testing.T, st kvrows.Store, ver, mid uint64, tid uint32,
	colKeys []engine.ColumnKey, cmds []transactedCmd) {

	txtbl := kvrows.MakeTransactedTable(st, getTx, ver, mid, tid, colKeys)
	sid := uint32(1)

	for _, cmd := range cmds {
		switch cmd.cmd {
		case cmdVersionedNextStmt:
			sid += 1
		case cmdVersionedInsert:
			err := txtbl.Insert(sid, cmd.row)
			if cmd.fail {
				if err == nil {
					t.Errorf("Insert(%v) did not fail", cmd.row)
				}
			} else if err != nil {
				t.Errorf("Insert(%v) failed with %s", cmd.row, err)
			}
		}
	}
}

func RunTransactedTableTest(t *testing.T, st kvrows.Store) {
	transactedCmds := []transactedCmd{
		{cmd: cmdVersionedInsert, row: []sql.Value{sql.Int64Value(1), sql.StringValue("one")}},
		{cmd: cmdVersionedInsert, row: []sql.Value{sql.Int64Value(2), sql.StringValue("two")}},
		{cmd: cmdVersionedInsert, row: []sql.Value{sql.Int64Value(3), sql.StringValue("three")}},
		{cmd: cmdVersionedInsert, row: []sql.Value{sql.Int64Value(4), sql.StringValue("four")}},
		{cmd: cmdVersionedInsert, fail: true,
			row: []sql.Value{sql.Int64Value(1), sql.StringValue("1")}},
	}

	testTransactedTable(t, st, 1, 1, 123, []engine.ColumnKey{engine.MakeColumnKey(0, false)},
		transactedCmds)
}
