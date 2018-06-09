package test

import (
	"context"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/fatlock"
	"github.com/leftmike/maho/sql"
)

type session struct{}

func (_ session) Context() context.Context {
	return nil
}

func (_ session) DefaultEngine() string {
	return ""
}

func (_ session) DefaultDatabase() sql.Identifier {
	return 0
}

var (
	int32ColType  = db.ColumnType{Type: sql.IntegerType, Size: 4, NotNull: true}
	int64ColType  = db.ColumnType{Type: sql.IntegerType, Size: 8, NotNull: true}
	boolColType   = db.ColumnType{Type: sql.BooleanType, NotNull: true}
	stringColType = db.ColumnType{Type: sql.CharacterType, Size: 4096, NotNull: true}
)

const (
	cmdBegin = iota
	cmdCommit
	cmdRollback
	cmdLookupTable
	cmdCreateTable
	cmdDropTable
	cmdListTables
)

type cmd struct {
	cmd              int
	fail             bool           // The command should fail
	needTransactions bool           // The test requires that transactions are supported
	exists           bool           // Flag for DropTable
	name             sql.Identifier // Name of the table
	list             []string       // List of table names
}

type testLocker struct {
	lockerState fatlock.LockerState
}

func (tl *testLocker) LockerState() *fatlock.LockerState {
	return &tl.lockerState
}

func testTableLifecycle(t *testing.T, d engine.Database, cmds []cmd) {
	var tctx interface{}
	var locker fatlock.Locker

	ses := session{}
	for _, cmd := range cmds {
		switch cmd.cmd {
		case cmdBegin:
			if tctx != nil {
				panic("tctx != nil: missing commit or rollback from commands")
			}
			locker = &testLocker{}
			tctx = d.Begin(locker)
			if tctx == nil && cmd.needTransactions {
				return // Engine does not support transactions, so skip these tests.
			}
		case cmdCommit:
			err := d.Commit(ses, tctx)
			if cmd.fail {
				if err == nil {
					t.Errorf("Commit() did not fail")
				}
			} else if err != nil {
				t.Errorf("Commit() failed with %s", err)
			}
			tctx = nil
			fatlock.ReleaseLocks(locker)
		case cmdRollback:
			err := d.Rollback(tctx)
			if cmd.fail {
				if err == nil {
					t.Errorf("Rollback() did not fail")
				}
			} else if err != nil {
				t.Errorf("Rollback() failed with %s", err)
			}
			tctx = nil
			fatlock.ReleaseLocks(locker)
		case cmdLookupTable:
			_, err := d.LookupTable(ses, tctx, cmd.name)
			if cmd.fail {
				if err == nil {
					t.Errorf("LookupTable(%s) did not fail", cmd.name)
				}
			} else if err != nil {
				t.Errorf("LookupTable(%s) failed with %s", cmd.name, err)
			}
		case cmdCreateTable:
			err := d.CreateTable(ses, tctx, cmd.name,
				[]sql.Identifier{sql.ID("col1"), sql.ID("col2"), sql.ID("col3"), sql.ID("col4")},
				[]db.ColumnType{int32ColType, int64ColType, boolColType, stringColType})
			if cmd.fail {
				if err == nil {
					t.Errorf("CreateTable(%s) did not fail", cmd.name)
				}
			} else if err != nil {
				t.Errorf("CreateTable(%s) failed with %s", cmd.name, err)
			}
		case cmdDropTable:
			err := d.DropTable(ses, tctx, cmd.name, cmd.exists)
			if cmd.fail {
				if err == nil {
					t.Errorf("DropTable(%s) did not fail", cmd.name)
				}
			} else if err != nil {
				t.Errorf("DropTable(%s) failed with %s", cmd.name, err)
			}
		case cmdListTables:
			entries, err := d.ListTables(ses, tctx)
			if err != nil {
				t.Errorf("ListTables() failed with %s", err)
			} else {
				var ret []string
				for _, te := range entries {
					ret = append(ret, te.Name.String())
				}
				sort.Strings(ret)
				if !reflect.DeepEqual(cmd.list, ret) {
					t.Errorf("ListTables() got %v want %v", ret, cmd.list)
				}
			}
		}
	}
}

func RunDatabaseTest(t *testing.T, e engine.Engine) {
	t.Helper()

	d, err := e.CreateDatabase(sql.ID("database_test"), filepath.Join("testdata", "database_test"),
		nil)
	if err != nil {
		t.Fatal(err)
	}

	_ = d.Message()

	testTableLifecycle(t, d,
		[]cmd{
			{cmd: cmdBegin},

			{cmd: cmdLookupTable, name: sql.ID("tbl-a"), fail: true},
			{cmd: cmdCreateTable, name: sql.ID("tbl-a")},
			{cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdListTables, list: []string{"tbl-a"}},
			{cmd: cmdCommit},
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: sql.ID("tbl-b")},
			{cmd: cmdCreateTable, name: sql.ID("tbl-c")},
			{cmd: cmdCreateTable, name: sql.ID("tbl-d")},
			{cmd: cmdListTables, list: []string{"tbl-a", "tbl-b", "tbl-c", "tbl-d"}},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdListTables, list: []string{"tbl-a", "tbl-b", "tbl-c", "tbl-d"}},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdDropTable, name: sql.ID("tbl-a")},
			{cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{cmd: cmdCommit},

			{cmd: cmdBegin, needTransactions: true},
			{cmd: cmdCreateTable, name: sql.ID("tbl-e")},
			{cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d", "tbl-e"}},
			{cmd: cmdRollback},

			{cmd: cmdBegin},
			{cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdDropTable, name: sql.ID("tbl-c")},
			{cmd: cmdListTables, list: []string{"tbl-b", "tbl-d"}},
			{cmd: cmdRollback},

			{cmd: cmdBegin},
			{cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{cmd: cmdCommit},
		})

	for i := 0; i < 2; i++ {
		testTableLifecycle(t, d,
			[]cmd{
				{cmd: cmdBegin},
				{cmd: cmdLookupTable, name: sql.ID("tbl1"), fail: true},
				{cmd: cmdCreateTable, name: sql.ID("tbl1")},
				{cmd: cmdLookupTable, name: sql.ID("tbl1")},
				{cmd: cmdCommit},
				{cmd: cmdBegin},
				{cmd: cmdLookupTable, name: sql.ID("tbl1")},
				{cmd: cmdDropTable, name: sql.ID("tbl1")},
				{cmd: cmdLookupTable, name: sql.ID("tbl1"), fail: true},
				{cmd: cmdCommit},
			})
	}

	testTableLifecycle(t, d,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdDropTable, name: sql.ID("tbl2"), exists: true},
			{cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, d,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdDropTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, d,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: sql.ID("tbl3")},
			{cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdDropTable, name: sql.ID("tbl3")},
			{cmd: cmdCreateTable, name: sql.ID("tbl3")},
			{cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{cmd: cmdCommit},
		})

	for i := 0; i < 2; i++ {
		testTableLifecycle(t, d,
			[]cmd{
				{cmd: cmdBegin},
				{cmd: cmdCreateTable, name: sql.ID("tbl4")},
				{cmd: cmdLookupTable, name: sql.ID("tbl4")},
				{cmd: cmdDropTable, name: sql.ID("tbl4")},
				{cmd: cmdLookupTable, name: sql.ID("tbl4"), fail: true},
				{cmd: cmdCommit},

				{cmd: cmdBegin},
				{cmd: cmdLookupTable, name: sql.ID("tbl4"), fail: true},
				{cmd: cmdCommit},
			})
	}

	testTableLifecycle(t, d,
		[]cmd{
			{cmd: cmdBegin, exists: true},
			{cmd: cmdCreateTable, name: sql.ID("tbl5")},
			{cmd: cmdLookupTable, name: sql.ID("tbl5")},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdDropTable, name: sql.ID("tbl5")},
			{cmd: cmdCreateTable, name: sql.ID("tbl5")},
			{cmd: cmdLookupTable, name: sql.ID("tbl5")},
			{cmd: cmdRollback},

			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl5")},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, d,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: sql.ID("tbl6")},
			{cmd: cmdLookupTable, name: sql.ID("tbl6")},
			{cmd: cmdDropTable, name: sql.ID("tbl6")},
			{cmd: cmdRollback},

			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl6"), fail: true},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, d,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: sql.ID("tbl7")},
			{cmd: cmdLookupTable, name: sql.ID("tbl7")},
			{cmd: cmdCommit},
		})

	for i := 0; i < 8; i++ {
		testTableLifecycle(t, d,
			[]cmd{
				{cmd: cmdBegin},
				{cmd: cmdLookupTable, name: sql.ID("tbl7")},
				{cmd: cmdDropTable, name: sql.ID("tbl7")},
				{cmd: cmdLookupTable, name: sql.ID("tbl7"), fail: true},
				{cmd: cmdCreateTable, name: sql.ID("tbl7")},
				{cmd: cmdLookupTable, name: sql.ID("tbl7")},
				{cmd: cmdCommit},
			})
	}

	testTableLifecycle(t, d,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl7")},
			{cmd: cmdCommit},
		})
}

func RunTableTest(t *testing.T, e engine.Engine) {
	t.Helper()

	ses := session{}

	d, err := e.CreateDatabase(sql.ID("database_test"), filepath.Join("testdata", "database_test"),
		nil)
	if err != nil {
		t.Fatal(err)
	}

	tblname := sql.ID("tbl1")
	testTableLifecycle(t, d,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: tblname},
			{cmd: cmdCommit},
		})

	tctx := d.Begin(&testLocker{})
	tbl, err := d.LookupTable(ses, tctx, tblname)
	if err != nil {
		t.Errorf("LookupTable(%s) failed with %s", tblname, err)
	}

	_ = tbl // XXX
}
