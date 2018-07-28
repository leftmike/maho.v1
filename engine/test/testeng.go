package test

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/fatlock"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

type session struct{}

func (_ session) Context() context.Context {
	return nil
}

type services struct {
	lockService fatlock.Service
}

func (svcs *services) Init() {
	svcs.lockService.Init()
}

func (svcs *services) LockService() fatlock.LockService {
	return &svcs.lockService
}

var (
	int32ColType  = sql.ColumnType{Type: sql.IntegerType, Size: 4, NotNull: true}
	int64ColType  = sql.ColumnType{Type: sql.IntegerType, Size: 8, NotNull: true}
	stringColType = sql.ColumnType{Type: sql.CharacterType, Size: 4096, NotNull: true}
)

const (
	cmdSession = iota
	cmdBegin
	cmdCommit
	cmdRollback
	cmdNextStmt
	cmdLookupTable
	cmdCreateTable
	cmdDropTable
	cmdListTables
	cmdRows
	cmdInsert
	cmdUpdate
	cmdDelete
)

type cmd struct {
	cmd              int
	ses              int                // Which session to use
	fail             bool               // The command should fail
	needTransactions bool               // The test requires that transactions are supported
	exists           bool               // Flag for DropTable
	name             sql.Identifier     // Name of the table
	list             []string           // List of table names
	values           [][]sql.Value      // Expected rows (table.Rows)
	row              []sql.Value        // Row to insert (table.Insert)
	rowID            int                // Row to update (rows.Update) or delete (rows.Delete)
	updates          []sql.ColumnUpdate // Updates to a row (rows.Update)
}

type testLocker struct {
	lockerState fatlock.LockerState
}

func (tl *testLocker) LockerState() *fatlock.LockerState {
	return &tl.lockerState
}

func (tl *testLocker) String() string {
	return fmt.Sprintf("locker-%p", tl)
}

type sessionState struct {
	ses    session
	tctx   interface{}
	locker fatlock.Locker
	tbl    engine.Table
	rows   engine.Rows
}

var (
	columns     = []sql.Identifier{sql.ID("ID"), sql.ID("intCol"), sql.ID("stringCol")}
	columnTypes = []sql.ColumnType{int32ColType, int64ColType, stringColType}
)

func allRows(t *testing.T, ses engine.Session, rows engine.Rows) [][]sql.Value {
	t.Helper()

	all := [][]sql.Value{}
	l := len(rows.Columns())
	for {
		dest := make([]sql.Value, l)
		err := rows.Next(ses, dest)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Errorf("rows.Next(): failed with %s", err)
			return nil
		}
		all = append(all, dest)
	}
	return all
}

func testTableLifecycle(t *testing.T, d engine.Database, svcs *services, cmds []cmd) {
	sessions := [4]sessionState{}
	state := &sessions[0]

	for _, cmd := range cmds {
		switch cmd.cmd {
		case cmdSession:
		case cmdNextStmt:
		case cmdRows:
		case cmdInsert:
		case cmdUpdate:
		case cmdDelete:
		default:
			state.tbl = nil
			if state.rows != nil {
				err := state.rows.Close()
				if err != nil {
					t.Errorf("rows.Close() failed with %s", err)
				}
				state.rows = nil
			}
		}

		switch cmd.cmd {
		case cmdSession:
			state = &sessions[cmd.ses]
		case cmdBegin:
			if state.tctx != nil {
				panic("tctx != nil: missing commit or rollback from commands")
			}
			state.locker = &testLocker{}
			state.tctx = d.Begin(state.locker)
			if state.tctx == nil && cmd.needTransactions {
				return // Engine does not support transactions, so skip these tests.
			}
		case cmdCommit:
			err := d.Commit(state.ses, state.tctx)
			if cmd.fail {
				if err == nil {
					t.Errorf("Commit() did not fail")
				}
			} else if err != nil {
				t.Errorf("Commit() failed with %s", err)
			}
			state.tctx = nil
			svcs.lockService.ReleaseLocks(state.locker)
		case cmdRollback:
			err := d.Rollback(state.tctx)
			if cmd.fail {
				if err == nil {
					t.Errorf("Rollback() did not fail")
				}
			} else if err != nil {
				t.Errorf("Rollback() failed with %s", err)
			}
			state.tctx = nil
			svcs.lockService.ReleaseLocks(state.locker)
		case cmdNextStmt:
			d.NextStmt(state.tctx)
		case cmdLookupTable:
			var err error
			state.tbl, err = d.LookupTable(state.ses, state.tctx, cmd.name)
			if cmd.fail {
				if err == nil {
					t.Errorf("LookupTable(%s) did not fail", cmd.name)
				}
			} else if err != nil {
				t.Errorf("LookupTable(%s) failed with %s", cmd.name, err)
			} else {
				cols := state.tbl.Columns(state.ses)
				if !reflect.DeepEqual(cols, columns) {
					t.Errorf("tbl.Columns() got %v want %v", cols, columns)
				}
				colTypes := state.tbl.ColumnTypes(state.ses)
				if !reflect.DeepEqual(colTypes, columnTypes) {
					t.Errorf("tbl.ColumnTypes() got %v want %v", colTypes, columnTypes)
				}
			}
		case cmdCreateTable:
			err := d.CreateTable(state.ses, state.tctx, cmd.name, columns, columnTypes)
			if cmd.fail {
				if err == nil {
					t.Errorf("CreateTable(%s) did not fail", cmd.name)
				}
			} else if err != nil {
				t.Errorf("CreateTable(%s) failed with %s", cmd.name, err)
			}
		case cmdDropTable:
			err := d.DropTable(state.ses, state.tctx, cmd.name, cmd.exists)
			if cmd.fail {
				if err == nil {
					t.Errorf("DropTable(%s) did not fail", cmd.name)
				}
			} else if err != nil {
				t.Errorf("DropTable(%s) failed with %s", cmd.name, err)
			}
		case cmdListTables:
			entries, err := d.ListTables(state.ses, state.tctx)
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
		case cmdRows:
			var err error
			state.rows, err = state.tbl.Rows(state.ses)
			if err != nil {
				t.Errorf("table.Rows() failed with %s", err)
			} else {
				vals := allRows(t, state.ses, state.rows)
				if vals != nil {
					testutil.SortValues(vals)
					if !reflect.DeepEqual(vals, cmd.values) {
						t.Errorf("table.Rows() got %v want %v", vals, cmd.values)
					}
				}
			}
		case cmdInsert:
			err := state.tbl.Insert(state.ses, cmd.row)
			if err != nil {
				t.Errorf("table.Insert() failed with %s", err)
			}
		case cmdUpdate:
			rows, err := state.tbl.Rows(state.ses)
			if err != nil {
				t.Errorf("table.Rows() failed with %s", err)
			} else {
				dest := make([]sql.Value, len(rows.Columns()))
				for {
					err = rows.Next(state.ses, dest)
					if err != nil {
						if !cmd.fail {
							t.Errorf("rows.Next() failed with %s", err)
						}
						break
					}
					if i64, ok := dest[0].(sql.Int64Value); ok && int(i64) == cmd.rowID {
						err = rows.Update(state.ses, cmd.updates)
						if cmd.fail {
							if err == nil {
								t.Errorf("rows.Update() did not fail")
							}
						} else if err != nil {
							t.Errorf("rows.Update() failed with %s", err)
						}
						break
					}
				}
			}
		case cmdDelete:
			rows, err := state.tbl.Rows(state.ses)
			if err != nil {
				t.Errorf("table.Rows() failed with %s", err)
			} else {
				dest := make([]sql.Value, len(rows.Columns()))
				for {
					err = rows.Next(state.ses, dest)
					if err != nil {
						if !cmd.fail {
							t.Errorf("rows.Next() failed with %s", err)
						}
						break
					}
					if i64, ok := dest[0].(sql.Int64Value); ok && int(i64) == cmd.rowID {
						err = rows.Delete(state.ses)
						if cmd.fail {
							if err == nil {
								t.Errorf("rows.Delete() did not fail")
							}
						} else if err != nil {
							t.Errorf("rows.Delete() failed with %s", err)
						}
						break
					}
				}
			}
		}
	}
}

func RunDatabaseTest(t *testing.T, e engine.Engine) {
	t.Helper()

	var svcs services
	svcs.Init()
	d, err := e.CreateDatabase(&svcs, sql.ID("database_test"),
		filepath.Join("testdata", "database_test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	_ = d.Message()

	testTableLifecycle(t, d, &svcs,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl-a"), fail: true},
			{cmd: cmdCreateTable, name: sql.ID("tbl-a")},
			{cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{cmd: cmdCreateTable, name: sql.ID("tbl-a"), fail: true},
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
		testTableLifecycle(t, d, &svcs,
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

	testTableLifecycle(t, d, &svcs,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdDropTable, name: sql.ID("tbl2"), exists: true},
			{cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, d, &svcs,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdDropTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, d, &svcs,
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
		testTableLifecycle(t, d, &svcs,
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

	testTableLifecycle(t, d, &svcs,
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

	testTableLifecycle(t, d, &svcs,
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

	testTableLifecycle(t, d, &svcs,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: sql.ID("tbl7")},
			{cmd: cmdLookupTable, name: sql.ID("tbl7")},
			{cmd: cmdCommit},
		})

	for i := 0; i < 8; i++ {
		testTableLifecycle(t, d, &svcs,
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

	testTableLifecycle(t, d, &svcs,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl7")},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, d, &svcs,
		[]cmd{
			{cmd: cmdSession, ses: 0},
			{cmd: cmdBegin},
			{cmd: cmdSession, ses: 1},
			{cmd: cmdBegin},
			{cmd: cmdSession, ses: 0},
			{cmd: cmdCreateTable, name: sql.ID("tbl8")},
			{cmd: cmdCommit},
			{cmd: cmdSession, ses: 1},
			{cmd: cmdCreateTable, name: sql.ID("tbl8"), fail: true},
			{cmd: cmdCommit},

			{cmd: cmdSession, ses: 0},
			{cmd: cmdBegin},
			{cmd: cmdSession, ses: 1},
			{cmd: cmdBegin},
			{cmd: cmdSession, ses: 0},
			{cmd: cmdCreateTable, name: sql.ID("tbl8"), fail: true},
			{cmd: cmdDropTable, name: sql.ID("tbl8")},
			{cmd: cmdDropTable, name: sql.ID("tbl8"), fail: true},
			{cmd: cmdCommit},
			{cmd: cmdSession, ses: 1},
			{cmd: cmdDropTable, name: sql.ID("tbl8"), fail: true},
			{cmd: cmdCommit},
		})
}

func RunTableTest(t *testing.T, e engine.Engine) {
	t.Helper()

	var svcs services
	svcs.Init()
	d, err := e.CreateDatabase(&svcs, sql.ID("table_test"),
		filepath.Join("testdata", "table_test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	testTableLifecycle(t, d, &svcs,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: sql.ID("tbl1")},
			{cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{cmd: cmdRows, values: [][]sql.Value{}},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{cmd: cmdRows, values: [][]sql.Value{}},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(1), sql.Int64Value(1),
				sql.StringValue("first row")}},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(2), sql.Int64Value(4),
				sql.StringValue("second row")}},
			{cmd: cmdNextStmt},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
				},
			},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
				},
			},
			{cmd: cmdCommit},

			{cmd: cmdBegin, needTransactions: true},
			{cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(3), sql.Int64Value(9),
				sql.StringValue("third row")}},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(4), sql.Int64Value(16),
				sql.StringValue("fourth row")}},
			{cmd: cmdNextStmt},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{cmd: cmdRollback},

			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
				},
			},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(3), sql.Int64Value(9),
				sql.StringValue("third row")}},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(4), sql.Int64Value(16),
				sql.StringValue("fourth row")}},
			{cmd: cmdNextStmt},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{cmd: cmdSession, ses: 1},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
				},
			},
			{cmd: cmdSession, ses: 0},
			{cmd: cmdCommit},
			{cmd: cmdSession, ses: 2},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{cmd: cmdCommit},
			{cmd: cmdSession, ses: 1},
			{cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
				},
			},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, d, &svcs,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: sql.ID("tbl2")},
			{cmd: cmdLookupTable, name: sql.ID("tbl2")},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(1), sql.Int64Value(1),
				sql.StringValue("first row")}},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(2), sql.Int64Value(4),
				sql.StringValue("second row")}},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(3), sql.Int64Value(9),
				sql.StringValue("third row")}},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(4), sql.Int64Value(16),
				sql.StringValue("fourth row")}},
			{cmd: cmdNextStmt},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl2")},
			{cmd: cmdDelete, rowID: 4},
			{cmd: cmdNextStmt},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
				},
			},
			{cmd: cmdCommit},

			{cmd: cmdBegin, needTransactions: true},
			{cmd: cmdLookupTable, name: sql.ID("tbl2")},
			{cmd: cmdDelete, rowID: 1},
			{cmd: cmdNextStmt},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
				},
			},
			{cmd: cmdSession, ses: 1},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl2")},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
				},
			},
			{cmd: cmdSession, ses: 0},
			{cmd: cmdCommit},
			{cmd: cmdSession, ses: 1},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
				},
			},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl2")},
			{cmd: cmdDelete, rowID: 2},
			{cmd: cmdNextStmt},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
				},
			},
			{cmd: cmdRollback},

			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl2")},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
				},
			},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, d, &svcs,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: sql.ID("tbl3")},
			{cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(1), sql.Int64Value(1),
				sql.StringValue("first row")}},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(2), sql.Int64Value(4),
				sql.StringValue("second row")}},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(3), sql.Int64Value(9),
				sql.StringValue("third row")}},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(4), sql.Int64Value(16),
				sql.StringValue("fourth row")}},
			{cmd: cmdNextStmt},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{cmd: cmdCommit},

			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{cmd: cmdUpdate, rowID: 1,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(10)}}},
			{cmd: cmdNextStmt},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(10), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{cmd: cmdCommit},

			{cmd: cmdBegin, needTransactions: true},
			{cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{cmd: cmdUpdate, rowID: 2,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(40)}}},
			{cmd: cmdNextStmt},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(10), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(40), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{cmd: cmdRollback},

			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(10), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{cmd: cmdCommit},

			{cmd: cmdSession, ses: 0},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{cmd: cmdUpdate, rowID: 3,
				updates: []sql.ColumnUpdate{
					{Index: 1, Value: sql.Int64Value(90)},
					{Index: 2, Value: sql.StringValue("3rd row")},
				},
			},
			{cmd: cmdSession, ses: 1},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(10), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{cmd: cmdCommit},
			{cmd: cmdSession, ses: 0},
			{cmd: cmdCommit},

			{cmd: cmdSession, ses: 1},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(10), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(90), sql.StringValue("3rd row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, d, &svcs,
		[]cmd{
			{cmd: cmdBegin, needTransactions: true},
			{cmd: cmdCreateTable, name: sql.ID("tbl4")},
			{cmd: cmdLookupTable, name: sql.ID("tbl4")},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(1), sql.Int64Value(1),
				sql.StringValue("first row")}},
			{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(2), sql.Int64Value(4),
				sql.StringValue("second row")}},
			{cmd: cmdNextStmt},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
				},
			},
			{cmd: cmdCommit},

			{cmd: cmdSession, ses: 0},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl4")},
			{cmd: cmdDelete, rowID: 1},
			{cmd: cmdSession, ses: 1},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl4")},
			{cmd: cmdDelete, rowID: 1, fail: true},
			{cmd: cmdUpdate, rowID: 1,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(40)}}, fail: true},
			{cmd: cmdCommit},
			{cmd: cmdSession, ses: 0},
			{cmd: cmdRollback},

			{cmd: cmdSession, ses: 0},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl4")},
			{cmd: cmdUpdate, rowID: 1,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(40)}}},
			{cmd: cmdSession, ses: 1},
			{cmd: cmdBegin},
			{cmd: cmdSession, ses: 0},
			{cmd: cmdCommit},
			{cmd: cmdSession, ses: 1},
			{cmd: cmdLookupTable, name: sql.ID("tbl4")},
			{cmd: cmdDelete, rowID: 1, fail: true},
			{cmd: cmdUpdate, rowID: 1,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(-40)}},
				fail:    true},
			{cmd: cmdCommit},

			{cmd: cmdSession, ses: 0},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl4")},
			{cmd: cmdUpdate, rowID: 1,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(400)}}},
			{cmd: cmdUpdate, rowID: 1,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(-400)}}, fail: true},
			{cmd: cmdNextStmt},
			{cmd: cmdUpdate, rowID: 1,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(4000)}}},
			{cmd: cmdCommit},
		})
}

func RunParallelTest(t *testing.T, e engine.Engine) {
	t.Helper()

	var svcs services
	svcs.Init()
	d, err := e.CreateDatabase(&svcs, sql.ID("parallel_test"),
		filepath.Join("testdata", "parallel_test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	testTableLifecycle(t, d, &svcs,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: sql.ID("tbl")},
			{cmd: cmdCommit},
		})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(t *testing.T, d engine.Database, i, r int) {
			defer wg.Done()

			for j := 0; j < r; j++ {
				testTableLifecycle(t, d, &svcs,
					[]cmd{
						{cmd: cmdBegin},
						{cmd: cmdLookupTable, name: sql.ID("tbl")},
						{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(i*r + j),
							sql.Int64Value(j), sql.StringValue(fmt.Sprintf("row %d.%d", i, j))}},
						{cmd: cmdCommit},
					})
			}

			for j := 0; j < r; j++ {
				testTableLifecycle(t, d, &svcs,
					[]cmd{
						{cmd: cmdBegin},
						{cmd: cmdLookupTable, name: sql.ID("tbl")},
						{cmd: cmdUpdate, rowID: i*r + j,
							updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(j * j)}}},
						{cmd: cmdCommit},
					})
			}
		}(t, d, i, 100)
	}
	wg.Wait()
}

func incColumn(t *testing.T, d engine.Database, tctx interface{}, i int, name sql.Identifier) bool {
	tbl, err := d.LookupTable(session{}, tctx, name)
	if err != nil {
		t.Fatalf("LookupTable(%s) failed with %s", name, err)
	}
	rows, err := tbl.Rows(session{})
	if err != nil {
		t.Fatalf("table.Rows() failed with %s", err)
	}

	dest := make([]sql.Value, len(rows.Columns()))
	for {
		err = rows.Next(session{}, dest)
		if err != nil {
			if err != io.EOF {
				t.Errorf("rows.Next() failed with %s", err)
			}
			break
		}
		if i64, ok := dest[0].(sql.Int64Value); ok && int(i64) == i {
			v := int(dest[1].(sql.Int64Value))
			err = rows.Update(session{},
				[]sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(v + 1)}})
			if err == nil {
				return true
			}
			break
		}
	}
	return false
}

func RunStressTest(t *testing.T, e engine.Engine) {
	t.Helper()

	var svcs services
	svcs.Init()
	d, err := e.CreateDatabase(&svcs, sql.ID("stress_test"),
		filepath.Join("testdata", "stress_test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	testTableLifecycle(t, d, &svcs,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: sql.ID("tbl")},
			{cmd: cmdCommit},
		})

	const rcnt = 100

	for i := 0; i < rcnt; i++ {
		testTableLifecycle(t, d, &svcs,
			[]cmd{
				{cmd: cmdBegin},
				{cmd: cmdLookupTable, name: sql.ID("tbl")},
				{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(i),
					sql.Int64Value(0), sql.StringValue(fmt.Sprintf("row %d", i))}},
				{cmd: cmdCommit},
			})
	}

	const tcnt = 20

	var wg sync.WaitGroup
	for i := 0; i < tcnt; i++ {
		wg.Add(1)
		go func(t *testing.T, d engine.Database, i, r int) {
			defer wg.Done()

			name := sql.ID("tbl")
			for i := 0; i < r; i++ {
				updated := false
				for !updated {
					lkr := &testLocker{}
					tctx := d.Begin(lkr)
					updated = incColumn(t, d, tctx, i, name)
					if updated {
						err := d.Commit(session{}, tctx)
						if err != nil {
							t.Errorf("Commit() failed with %s", err)
						}
					} else {
						err := d.Rollback(tctx)
						if err != nil {
							t.Errorf("Rollback() failed with %s", err)
						}
					}
				}
			}
		}(t, d, i, rcnt)
	}
	wg.Wait()

	var values [][]sql.Value
	for i := 0; i < rcnt; i++ {
		values = append(values, []sql.Value{sql.Int64Value(i), sql.Int64Value(tcnt),
			sql.StringValue(fmt.Sprintf("row %d", i))})
	}

	testTableLifecycle(t, d, &svcs,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl")},
			{cmd: cmdRows, values: values},
			{cmd: cmdCommit},
		})
}
