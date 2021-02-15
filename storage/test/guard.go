package test

import (
	"reflect"
	"sync"
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

type step struct {
	thrd int
	sync bool
	test interface{}
}

var (
	guardCmds = [][]storeCmd{
		[]storeCmd{
			{thrd: 0, fln: fln(), cmd: cmdBegin},
			{thrd: 0, fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl1"),
				cols:     []sql.Identifier{sql.ID("ID"), sql.ID("col2")},
				colTypes: []sql.ColumnType{int32ColType, int64ColType},
				key:      []sql.ColumnKey{sql.MakeColumnKey(0, false)},
			},
			{thrd: 0, fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl2"),
				cols:     []sql.Identifier{sql.ID("ID"), sql.ID("col2")},
				colTypes: []sql.ColumnType{int32ColType, int64ColType},
				key:      []sql.ColumnKey{sql.MakeColumnKey(0, false)},
			},
			{thrd: 0, fln: fln(), cmd: cmdCommit},
		},

		insertRows(sql.ID("tbl1"),
			[][]sql.Value{
				{i64Val(1), i64Val(10)},
				{i64Val(2), i64Val(20)},
				{i64Val(3), i64Val(30)},
				{i64Val(4), i64Val(10)},
				{i64Val(5), i64Val(20)},
				{i64Val(6), i64Val(30)},
				{i64Val(7), i64Val(10)},
			}),

		insertRows(sql.ID("tbl2"),
			[][]sql.Value{
				{i64Val(10), i64Val(1)},
				{i64Val(20), i64Val(2)},
				{i64Val(30), i64Val(3)},
				{i64Val(40), i64Val(4)},
				{i64Val(50), i64Val(5)},
				{i64Val(60), i64Val(6)},
			}),

		[]storeCmd{
			{thrd: 0, fln: fln(), cmd: cmdSync},
			{thrd: 1, fln: fln(), cmd: cmdBegin},
		},

		checkReference(1, sql.ID("tbl2"), 30, false),

		[]storeCmd{
			{thrd: 1, fln: fln(), cmd: cmdCommit},

			{thrd: 0, fln: fln(), cmd: cmdBegin},
		},

		checkReference(0, sql.ID("tbl2"), 90, true),

		[]storeCmd{
			{thrd: 0, fln: fln(), cmd: cmdRollback},

			{thrd: 1, fln: fln(), cmd: cmdSync},
			{thrd: 0, fln: fln(), cmd: cmdSync},

			{thrd: 0, fln: fln(), cmd: cmdBegin},
			{thrd: 0, fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{thrd: 0, fln: fln(), cmd: cmdInsert, row: []sql.Value{i64Val(8), i64Val(20)}},
		},

		checkReference(0, sql.ID("tbl2"), 20, false),

		[]storeCmd{
			{thrd: 0, fln: fln(), cmd: cmdCommit},
			{thrd: 0, fln: fln(), cmd: cmdSync},

			{thrd: 1, fln: fln(), cmd: cmdBegin},
			{thrd: 1, fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{thrd: 1, fln: fln(), cmd: cmdInsert, row: []sql.Value{i64Val(9), i64Val(90)}},
		},

		checkReference(1, sql.ID("tbl2"), 90, true),

		[]storeCmd{
			{thrd: 1, fln: fln(), cmd: cmdRollback},
			{thrd: 1, fln: fln(), cmd: cmdSync},
		},

		modifyReference(1, sql.ID("tbl1"), sql.ID("tbl2"), 10, 10, true),

		[]storeCmd{
			{thrd: 1, fln: fln(), cmd: cmdSync},
		},

		modifyReference(0, sql.ID("tbl1"), sql.ID("tbl2"), 60, 60, false),

		[]storeCmd{
			{thrd: 0, fln: fln(), cmd: cmdSync},
		},

		modifyReference(0, sql.ID("tbl1"), sql.ID("tbl2"), 50, 60, false),

		[]storeCmd{
			{thrd: 0, fln: fln(), cmd: cmdSync},

			{thrd: 0, fln: fln(), cmd: cmdBegin},
			{thrd: 0, fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{thrd: 0, fln: fln(), cmd: cmdInsert, row: []sql.Value{i64Val(9), i64Val(50)}},
		},

		checkReference(0, sql.ID("tbl2"), 50, true),

		[]storeCmd{
			{thrd: 0, fln: fln(), cmd: cmdRollback},

			{thrd: 0, fln: fln(), cmd: cmdBegin},
			{thrd: 0, fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{thrd: 0, fln: fln(), cmd: cmdInsert, row: []sql.Value{i64Val(9), i64Val(60)}},
		},

		checkReference(0, sql.ID("tbl2"), 60, false),

		[]storeCmd{
			{thrd: 0, fln: fln(), cmd: cmdSync},
		},

		// XXX: test if guards work
		// modifyReference(1, sql.ID("tbl1"), sql.ID("tbl2"), 60, 60, true),

		[]storeCmd{
			{thrd: 0, fln: fln(), cmd: cmdCommit},
			{thrd: 0, fln: fln(), cmd: cmdSync},
			{thrd: 1, fln: fln(), cmd: cmdSync},
		},
	}
)

func checkReference(thrd int, rnam sql.Identifier, val int, fail bool) []storeCmd {
	keyRow := []sql.Value{i64Val(val), nil}
	if fail {
		return []storeCmd{
			{thrd: thrd, fln: fln(), cmd: cmdLookupTable, name: rnam},
			{thrd: thrd, fln: fln(), cmd: cmdRows, guard: true, minRow: keyRow, maxRow: keyRow},
		}
	} else {
		return []storeCmd{
			{thrd: thrd, fln: fln(), cmd: cmdLookupTable, name: rnam},
			{thrd: thrd, fln: fln(), cmd: cmdRows, guard: true, minRow: keyRow, maxRow: keyRow,
				values: [][]sql.Value{{i64Val(val), i64Val(val / 10)}}},
		}
	}
}

func modifyReference(thrd int, fknam, rnam sql.Identifier, val, nval int, fail bool) []storeCmd {
	var modCmd storeCmd
	if val == nval {
		modCmd = storeCmd{thrd: thrd, fln: fln(), cmd: cmdDelete, rowID: val}
	} else {
		modCmd = storeCmd{
			thrd:  thrd,
			fln:   fln(),
			cmd:   cmdUpdate,
			rowID: val,
			updates: []sql.ColumnUpdate{
				{Column: 0, Value: i64Val(nval)},
				{Column: 1, Value: i64Val(nval / 10)},
			},
		}
	}
	cmds := []storeCmd{
		{thrd: thrd, fln: fln(), cmd: cmdBegin},
		{thrd: thrd, fln: fln(), cmd: cmdLookupTable, name: rnam},
		modCmd,
		{thrd: thrd, fln: fln(), cmd: cmdLookupTable, name: fknam},
		{thrd: thrd, fln: fln(), cmd: cmdRows, guard: true, fail: fail,
			rowsCheck: func(vals [][]sql.Value) bool {
				for _, row := range vals {
					if reflect.DeepEqual(row[1], i64Val(val)) {
						return true
					}
				}
				return false
			},
		},
	}
	if fail {
		return append(cmds, storeCmd{thrd: thrd, fln: fln(), cmd: cmdRollback})
	}
	return append(cmds, storeCmd{thrd: thrd, fln: fln(), cmd: cmdCommit})
}

func runAsyncTest(t *testing.T, st *storage.Store, dbname sql.Identifier, steps [][]storeCmd) {
	var thrds [4]chan storeCmd
	var syncs [4]chan struct{}
	var thrd int
	var wg sync.WaitGroup

	t.Helper()

	err := st.CreateDatabase(dbname, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, cmds := range steps {
		for _, cmd := range cmds {
			if cmd.thrd != thrd {
				if cmd.thrd >= len(thrds) {
					t.Fatalf("cmd.thrd = %d; len(thrds) = %d", cmd.thrd, len(thrds))
				}
				thrd = cmd.thrd
			}

			if thrds[thrd] == nil {
				thrds[thrd] = make(chan storeCmd, 10)
				syncs[thrd] = make(chan struct{})
				wg.Add(1)

				go func(cmds <-chan storeCmd, sync chan<- struct{}) {
					defer wg.Done()

					testDatabaseCmds(t, st, dbname, sync,
						func() (storeCmd, bool) {
							cmd, ok := <-cmds
							return cmd, ok
						})
				}(thrds[thrd], syncs[thrd])
			}

			thrds[thrd] <- cmd
			if cmd.cmd == cmdSync {
				<-syncs[thrd]
			}
		}
	}

	for _, thrd := range thrds {
		if thrd != nil {
			close(thrd)
		}
	}

	wg.Wait()
}

func RunGuardTest(t *testing.T, st *storage.Store) {
	runAsyncTest(t, st, sql.ID("guard_test"), guardCmds)
}
