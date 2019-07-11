package test

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

var (
	int32ColType  = sql.ColumnType{Type: sql.IntegerType, Size: 4, NotNull: true}
	int64ColType  = sql.ColumnType{Type: sql.IntegerType, Size: 8, NotNull: true}
	stringColType = sql.ColumnType{Type: sql.CharacterType, Size: 4096, NotNull: true}
)

const (
	cmdTransaction = iota
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
	tdx              int                // Which transaction to use
	fail             bool               // The command should fail
	needTransactions bool               // The test requires that transactions are supported
	ifExists         bool               // Flag for DropTable
	name             sql.Identifier     // Name of the table
	list             []string           // List of table names
	values           [][]sql.Value      // Expected rows (table.Rows)
	row              []sql.Value        // Row to insert (table.Insert)
	rowID            int                // Row to update (rows.Update) or delete (rows.Delete)
	updates          []sql.ColumnUpdate // Updates to a row (rows.Update)
}

type transactionState struct {
	tdx  int
	tx   engine.Transaction
	tbl  engine.Table
	rows engine.Rows
}

var (
	columns     = []sql.Identifier{sql.ID("ID"), sql.ID("intCol"), sql.ID("stringCol")}
	columnTypes = []sql.ColumnType{int32ColType, int64ColType, stringColType}
)

func allRows(t *testing.T, ctx context.Context, rows engine.Rows) [][]sql.Value {
	t.Helper()

	all := [][]sql.Value{}
	l := len(rows.Columns())
	for {
		dest := make([]sql.Value, l)
		err := rows.Next(ctx, dest)
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

func testTableLifecycle(t *testing.T, e engine.Engine, dbname sql.Identifier, cmds []cmd) {
	transactions := [4]transactionState{}
	state := &transactions[0]
	state.tdx = 0

	var ctx context.Context

	for _, cmd := range cmds {
		switch cmd.cmd {
		case cmdTransaction:
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
		case cmdTransaction:
			state = &transactions[cmd.tdx]
			state.tdx = cmd.tdx
		case cmdBegin:
			if state.tx != nil {
				panic("tx != nil: missing commit or rollback from commands")
			}
			if cmd.needTransactions && !e.IsTransactional() {
				return // Engine does not support transactions, so skip these tests.
			}
			state.tx = e.Begin(uint64(state.tdx))
		case cmdCommit:
			err := state.tx.Commit(ctx)
			if cmd.fail {
				if err == nil {
					t.Errorf("Commit() did not fail")
				}
			} else if err != nil {
				t.Errorf("Commit() failed with %s", err)
			}
			state.tx = nil
		case cmdRollback:
			err := state.tx.Rollback()
			if cmd.fail {
				if err == nil {
					t.Errorf("Rollback() did not fail")
				}
			} else if err != nil {
				t.Errorf("Rollback() failed with %s", err)
			}
			state.tx = nil
		case cmdNextStmt:
			state.tx.NextStmt()
		case cmdLookupTable:
			var err error
			state.tbl, err = e.LookupTable(ctx, state.tx, sql.TableName{dbname, cmd.name})
			if cmd.fail {
				if err == nil {
					t.Errorf("LookupTable(%s) did not fail", cmd.name)
				}
			} else if err != nil {
				t.Errorf("LookupTable(%s) failed with %s", cmd.name, err)
			} else {
				cols := state.tbl.Columns(ctx)
				if !reflect.DeepEqual(cols, columns) {
					t.Errorf("tbl.Columns() got %v want %v", cols, columns)
				}
				colTypes := state.tbl.ColumnTypes(ctx)
				if !reflect.DeepEqual(colTypes, columnTypes) {
					t.Errorf("tbl.ColumnTypes() got %v want %v", colTypes, columnTypes)
				}
			}
		case cmdCreateTable:
			err := e.CreateTable(ctx, state.tx, sql.TableName{dbname, cmd.name}, columns,
				columnTypes)
			if cmd.fail {
				if err == nil {
					t.Errorf("CreateTable(%s) did not fail", cmd.name)
				}
			} else if err != nil {
				t.Errorf("CreateTable(%s) failed with %s", cmd.name, err)
			}
		case cmdDropTable:
			err := e.DropTable(ctx, state.tx, sql.TableName{dbname, cmd.name}, cmd.ifExists)
			if cmd.fail {
				if err == nil {
					t.Errorf("DropTable(%s) did not fail", cmd.name)
				}
			} else if err != nil {
				t.Errorf("DropTable(%s) failed with %s", cmd.name, err)
			}
		case cmdListTables:
			ve, ok := e.(virtual.Engine)
			if !ok {
				break
			}
			tblnames, err := ve.ListTables(ctx, state.tx, dbname)
			if err != nil {
				t.Errorf("ListTables() failed with %s", err)
			} else {
				var ret []string
				for _, tblname := range tblnames {
					ret = append(ret, tblname.String())
				}
				sort.Strings(ret)
				if !reflect.DeepEqual(cmd.list, ret) {
					t.Errorf("ListTables() got %v want %v", ret, cmd.list)
				}
			}
		case cmdRows:
			var err error
			state.rows, err = state.tbl.Rows(ctx)
			if err != nil {
				t.Errorf("table.Rows() failed with %s", err)
			} else {
				vals := allRows(t, ctx, state.rows)
				if vals != nil {
					testutil.SortValues(vals)
					if !reflect.DeepEqual(vals, cmd.values) {
						t.Errorf("table.Rows() got %v want %v", vals, cmd.values)
					}
				}
			}
		case cmdInsert:
			err := state.tbl.Insert(ctx, cmd.row)
			if err != nil {
				t.Errorf("table.Insert() failed with %s", err)
			}
		case cmdUpdate:
			rows, err := state.tbl.Rows(ctx)
			if err != nil {
				t.Errorf("table.Rows() failed with %s", err)
			} else {
				dest := make([]sql.Value, len(rows.Columns()))
				for {
					err = rows.Next(ctx, dest)
					if err != nil {
						if !cmd.fail {
							t.Errorf("rows.Next() failed with %s", err)
						}
						break
					}
					if i64, ok := dest[0].(sql.Int64Value); ok && int(i64) == cmd.rowID {
						err = rows.Update(ctx, cmd.updates)
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
			rows, err := state.tbl.Rows(ctx)
			if err != nil {
				t.Errorf("table.Rows() failed with %s", err)
			} else {
				dest := make([]sql.Value, len(rows.Columns()))
				for {
					err = rows.Next(ctx, dest)
					if err != nil {
						if !cmd.fail {
							t.Errorf("rows.Next() failed with %s", err)
						}
						break
					}
					if i64, ok := dest[0].(sql.Int64Value); ok && int(i64) == cmd.rowID {
						err = rows.Delete(ctx)
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

	dbname := sql.ID("database_test")
	err := e.CreateDatabase(dbname, nil)
	if err != nil {
		t.Fatal(err)
	}

	testTableLifecycle(t, e, dbname,
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
		testTableLifecycle(t, e, dbname,
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

	testTableLifecycle(t, e, dbname,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdDropTable, name: sql.ID("tbl2"), ifExists: true},
			{cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, e, dbname,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdDropTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, e, dbname,
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
		testTableLifecycle(t, e, dbname,
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

	testTableLifecycle(t, e, dbname,
		[]cmd{
			{cmd: cmdBegin},
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

	testTableLifecycle(t, e, dbname,
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

	testTableLifecycle(t, e, dbname,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: sql.ID("tbl7")},
			{cmd: cmdLookupTable, name: sql.ID("tbl7")},
			{cmd: cmdCommit},
		})

	for i := 0; i < 8; i++ {
		testTableLifecycle(t, e, dbname,
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

	testTableLifecycle(t, e, dbname,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl7")},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, e, dbname,
		[]cmd{
			{cmd: cmdTransaction, tdx: 0},
			{cmd: cmdBegin},
			{cmd: cmdTransaction, tdx: 1},
			{cmd: cmdBegin},
			{cmd: cmdTransaction, tdx: 0},
			{cmd: cmdCreateTable, name: sql.ID("tbl8")},
			{cmd: cmdCommit},
			{cmd: cmdTransaction, tdx: 1},
			{cmd: cmdCreateTable, name: sql.ID("tbl8"), fail: true},
			{cmd: cmdCommit},

			{cmd: cmdTransaction, tdx: 0},
			{cmd: cmdBegin},
			{cmd: cmdTransaction, tdx: 1},
			{cmd: cmdBegin},
			{cmd: cmdTransaction, tdx: 0},
			{cmd: cmdCreateTable, name: sql.ID("tbl8"), fail: true},
			{cmd: cmdDropTable, name: sql.ID("tbl8")},
			{cmd: cmdDropTable, name: sql.ID("tbl8"), fail: true},
			{cmd: cmdCommit},
			{cmd: cmdTransaction, tdx: 1},
			{cmd: cmdDropTable, name: sql.ID("tbl8"), fail: true},
			{cmd: cmdCommit},
		})
}

func RunTableTest(t *testing.T, e engine.Engine) {
	t.Helper()

	dbname := sql.ID("table_test")
	err := e.CreateDatabase(dbname, nil)
	if err != nil {
		t.Fatal(err)
	}

	testTableLifecycle(t, e, dbname,
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
			{cmd: cmdTransaction, tdx: 1},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
				},
			},
			{cmd: cmdTransaction, tdx: 0},
			{cmd: cmdCommit},
			{cmd: cmdTransaction, tdx: 2},
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
			{cmd: cmdTransaction, tdx: 1},
			{cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
				},
			},
			{cmd: cmdCommit},
		})

	testTableLifecycle(t, e, dbname,
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
			{cmd: cmdTransaction, tdx: 1},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl2")},
			{cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
				},
			},
			{cmd: cmdTransaction, tdx: 0},
			{cmd: cmdCommit},
			{cmd: cmdTransaction, tdx: 1},
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

	testTableLifecycle(t, e, dbname,
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

			{cmd: cmdTransaction, tdx: 0},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{cmd: cmdUpdate, rowID: 3,
				updates: []sql.ColumnUpdate{
					{Index: 1, Value: sql.Int64Value(90)},
					{Index: 2, Value: sql.StringValue("3rd row")},
				},
			},
			{cmd: cmdTransaction, tdx: 1},
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
			{cmd: cmdTransaction, tdx: 0},
			{cmd: cmdCommit},

			{cmd: cmdTransaction, tdx: 1},
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

	testTableLifecycle(t, e, dbname,
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

			{cmd: cmdTransaction, tdx: 0},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl4")},
			{cmd: cmdDelete, rowID: 1},
			{cmd: cmdTransaction, tdx: 1},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl4")},
			{cmd: cmdDelete, rowID: 1, fail: true},
			{cmd: cmdUpdate, rowID: 1,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(40)}}, fail: true},
			{cmd: cmdCommit},
			{cmd: cmdTransaction, tdx: 0},
			{cmd: cmdRollback},

			{cmd: cmdTransaction, tdx: 0},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl4")},
			{cmd: cmdUpdate, rowID: 1,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(40)}}},
			{cmd: cmdTransaction, tdx: 1},
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl4")},
			{cmd: cmdDelete, rowID: 1, fail: true},
			{cmd: cmdUpdate, rowID: 1,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(-40)}},
				fail:    true},
			{cmd: cmdTransaction, tdx: 0},
			{cmd: cmdCommit},
			{cmd: cmdTransaction, tdx: 1},
			{cmd: cmdCommit},

			{cmd: cmdTransaction, tdx: 0},
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

	dbname := sql.ID("parallel_test")
	err := e.CreateDatabase(sql.ID("parallel_test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	testTableLifecycle(t, e, dbname,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: sql.ID("tbl")},
			{cmd: cmdCommit},
		})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i, r int) {
			defer wg.Done()

			for j := 0; j < r; j++ {
				testTableLifecycle(t, e, dbname,
					[]cmd{
						{cmd: cmdBegin},
						{cmd: cmdLookupTable, name: sql.ID("tbl")},
						{cmd: cmdInsert, row: []sql.Value{sql.Int64Value(i*r + j),
							sql.Int64Value(j), sql.StringValue(fmt.Sprintf("row %d.%d", i, j))}},
						{cmd: cmdCommit},
					})
			}

			for j := 0; j < r; j++ {
				testTableLifecycle(t, e, dbname,
					[]cmd{
						{cmd: cmdBegin},
						{cmd: cmdLookupTable, name: sql.ID("tbl")},
						{cmd: cmdUpdate, rowID: i*r + j,
							updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(j * j)}}},
						{cmd: cmdCommit},
					})
			}
		}(i, 100)
	}
	wg.Wait()
}

func incColumn(t *testing.T, e engine.Engine, tx engine.Transaction, i int, tn sql.TableName) bool {

	var ctx context.Context

	tbl, err := e.LookupTable(ctx, tx, tn)
	if err != nil {
		t.Fatalf("LookupTable(%s) failed with %s", tn, err)
	}
	rows, err := tbl.Rows(ctx)
	if err != nil {
		t.Fatalf("table.Rows() failed with %s", err)
	}

	dest := make([]sql.Value, len(rows.Columns()))
	for {
		err = rows.Next(ctx, dest)
		if err != nil {
			if err != io.EOF {
				t.Errorf("rows.Next() failed with %s", err)
			}
			break
		}
		if i64, ok := dest[0].(sql.Int64Value); ok && int(i64) == i {
			v := int(dest[1].(sql.Int64Value))
			err = rows.Update(ctx,
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

	dbname := sql.ID("stress_test")
	err := e.CreateDatabase(dbname, nil)
	if err != nil {
		t.Fatal(err)
	}

	testTableLifecycle(t, e, dbname,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdCreateTable, name: sql.ID("tbl")},
			{cmd: cmdCommit},
		})

	const rcnt = 100

	for i := 0; i < rcnt; i++ {
		testTableLifecycle(t, e, dbname,
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
	for tdx := uint64(0); tdx < tcnt; tdx++ {
		wg.Add(1)
		go func(tdx uint64, r int) {
			defer wg.Done()

			var ctx context.Context
			name := sql.ID("tbl")
			for i := 0; i < r; i++ {
				updated := false
				for !updated {
					tx := e.Begin(tdx)
					updated = incColumn(t, e, tx, i, sql.TableName{dbname, name})
					if updated {
						err := tx.Commit(ctx)
						if err != nil {
							t.Errorf("Commit() failed with %s", err)
						}
					} else {
						err := tx.Rollback()
						if err != nil {
							t.Errorf("Rollback() failed with %s", err)
						}
					}
				}
			}
		}(tdx, rcnt)
	}
	wg.Wait()

	var values [][]sql.Value
	for i := 0; i < rcnt; i++ {
		values = append(values, []sql.Value{sql.Int64Value(i), sql.Int64Value(tcnt),
			sql.StringValue(fmt.Sprintf("row %d", i))})
	}

	testTableLifecycle(t, e, dbname,
		[]cmd{
			{cmd: cmdBegin},
			{cmd: cmdLookupTable, name: sql.ID("tbl")},
			{cmd: cmdRows, values: values},
			{cmd: cmdCommit},
		})
}
