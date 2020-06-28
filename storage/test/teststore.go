package test

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/testutil"
)

var (
	int32ColType  = sql.ColumnType{Type: sql.IntegerType, Size: 4, NotNull: true}
	int64ColType  = sql.ColumnType{Type: sql.IntegerType, Size: 8, NotNull: true}
	stringColType = sql.ColumnType{Type: sql.StringType, Size: 4096, NotNull: true}
)

const (
	cmdBegin = iota
	cmdCommit
	cmdRollback
	cmdNextStmt
	cmdCreateSchema
	cmdDropSchema
	cmdSetSchema
	cmdListSchemas
	cmdLookupSchema
	cmdLookupTable
	cmdCreateTable
	cmdDropTable
	cmdListTables
	cmdRows
	cmdInsert
	cmdUpdate
	cmdDelete
	cmdCreateDatabase
	cmdDropDatabase
)

func fln() testutil.FileLineNumber {
	return testutil.MakeFileLineNumber()
}

type storeCmd struct {
	fln      testutil.FileLineNumber
	cmd      int
	tdx      int                // Which transaction to use
	fail     bool               // The command should fail
	ifExists bool               // Flag for DropTable or DropSchema
	name     sql.Identifier     // Name of the table or schema
	list     []string           // List of table names or schema names
	values   [][]sql.Value      // Expected rows (table.Rows)
	row      []sql.Value        // Row to insert (table.Insert)
	rowID    int                // Row to update (rows.Update) or delete (rows.Delete)
	updates  []sql.ColumnUpdate // Updates to a row (rows.Update)
}

type transactionState struct {
	tdx int
	tx  storage.Transaction
	tbl storage.Table
}

var (
	columns     = []sql.Identifier{sql.ID("ID"), sql.ID("intCol"), sql.ID("stringCol")}
	columnTypes = []sql.ColumnType{int32ColType, int64ColType, stringColType}
	primary     = []sql.ColumnKey{sql.MakeColumnKey(0, false)}
)

func allRows(t *testing.T, ctx context.Context, rows sql.Rows,
	fln testutil.FileLineNumber) [][]sql.Value {

	t.Helper()

	all := [][]sql.Value{}
	l := len(rows.Columns())
	for {
		dest := make([]sql.Value, l)
		err := rows.Next(ctx, dest)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Errorf("%srows.Next(): failed with %s", fln, err)
			return nil
		}
		all = append(all, dest)
	}

	err := rows.Close()
	if err != nil {
		t.Errorf("%srows.Close(): failed with %s", fln, err)
	}
	return all
}

func testDatabase(t *testing.T, st storage.Store, dbname sql.Identifier, cmds []storeCmd) {
	var state transactionState
	var ctx context.Context
	scname := sql.PUBLIC

	for _, cmd := range cmds {
		//fmt.Printf("%s%d\n", cmd.fln, cmd.cmd)
		switch cmd.cmd {
		case cmdNextStmt:
		case cmdRows:
		case cmdInsert:
		case cmdUpdate:
		case cmdDelete:
		default:
			state.tbl = nil
		}

		switch cmd.cmd {
		case cmdBegin:
			if state.tx != nil {
				panic("tx != nil: missing commit or rollback from commands")
			}
			state.tx = st.Begin(uint64(state.tdx))
			if state.tx == nil {
				t.Errorf("%sBegin() failed", cmd.fln)
				return
			}
		case cmdCommit:
			err := state.tx.Commit(ctx)
			if cmd.fail {
				if err == nil {
					t.Errorf("%sCommit() did not fail", cmd.fln)
				}
			} else if err != nil {
				t.Errorf("%sCommit() failed with %s", cmd.fln, err)
			}
			state.tx = nil
		case cmdRollback:
			err := state.tx.Rollback()
			if cmd.fail {
				if err == nil {
					t.Errorf("%sRollback() did not fail", cmd.fln)
				}
			} else if err != nil {
				t.Errorf("%sRollback() failed with %s", cmd.fln, err)
			}
			state.tx = nil
		case cmdNextStmt:
			state.tx.NextStmt()
		case cmdCreateSchema:
			err := st.CreateSchema(ctx, state.tx, sql.SchemaName{dbname, cmd.name})
			if cmd.fail {
				if err == nil {
					t.Errorf("%sCreateSchema(%s) did not fail", cmd.fln, cmd.name)
				}
			} else if err != nil {
				t.Errorf("%sCreateSchema(%s) failed with %s", cmd.fln, cmd.name, err)
			}
			state.tx.NextStmt()
		case cmdDropSchema:
			err := st.DropSchema(ctx, state.tx, sql.SchemaName{dbname, cmd.name}, cmd.ifExists)
			if cmd.fail {
				if err == nil {
					t.Errorf("%sDropSchema(%s) did not fail", cmd.fln, cmd.name)
				}
			} else if err != nil {
				t.Errorf("%sDropSchema(%s) failed with %s", cmd.fln, cmd.name, err)
			}
			state.tx.NextStmt()
		case cmdSetSchema:
			scname = cmd.name
		case cmdListSchemas:
			scnames, err := st.ListSchemas(ctx, state.tx, dbname)
			if err != nil {
				t.Errorf("%sListSchemas() failed with %s", cmd.fln, err)
			} else {
				var ret []string
				for _, scname := range scnames {
					ret = append(ret, scname.String())
				}
				sort.Strings(ret)
				if !reflect.DeepEqual(cmd.list, ret) {
					t.Errorf("%sListSchemas() got %v want %v", cmd.fln, ret, cmd.list)
				}
			}
		case cmdLookupSchema:
			scnames, err := st.ListSchemas(ctx, state.tx, dbname)
			if err != nil {
				t.Errorf("%sListSchemas() failed with %s", cmd.fln, err)
			} else {
				found := false
				for _, scname := range scnames {
					if scname == cmd.name {
						found = true
						break
					}
				}
				if cmd.fail {
					if found {
						t.Errorf("%sLookupSchema(%s) did not fail", cmd.fln, cmd.name)
					}
				} else if !found {
					t.Errorf("%sLookupSchema(%s): schema not found", cmd.fln, cmd.name)
				}
			}
		case cmdLookupTable:
			var err error
			state.tbl, err = st.LookupTable(ctx, state.tx,
				sql.TableName{dbname, scname, cmd.name})
			if cmd.fail {
				if err == nil {
					t.Errorf("%sLookupTable(%s) did not fail", cmd.fln, cmd.name)
				}
			} else if err != nil {
				t.Errorf("%sLookupTable(%s) failed with %s", cmd.fln, cmd.name, err)
			} else {
				cols := state.tbl.Columns(ctx)
				if !reflect.DeepEqual(cols, columns) {
					t.Errorf("%stbl.Columns() got %v want %v", cmd.fln, cols, columns)
				}
				colTypes := state.tbl.ColumnTypes(ctx)
				if !reflect.DeepEqual(colTypes, columnTypes) {
					t.Errorf("%stbl.ColumnTypes() got %v want %v", cmd.fln, colTypes, columnTypes)
				}
			}
		case cmdCreateTable:
			err := st.CreateTable(ctx, state.tx, sql.TableName{dbname, scname, cmd.name},
				columns, columnTypes, primary, false)
			if cmd.fail {
				if err == nil {
					t.Errorf("%sCreateTable(%s) did not fail", cmd.fln, cmd.name)
				}
			} else if err != nil {
				t.Errorf("%sCreateTable(%s) failed with %s", cmd.fln, cmd.name, err)
			}
			state.tx.NextStmt()
		case cmdDropTable:
			err := st.DropTable(ctx, state.tx, sql.TableName{dbname, scname, cmd.name},
				cmd.ifExists)
			if cmd.fail {
				if err == nil {
					t.Errorf("%sDropTable(%s) did not fail", cmd.fln, cmd.name)
				}
			} else if err != nil {
				t.Errorf("%sDropTable(%s) failed with %s", cmd.fln, cmd.name, err)
			}
			state.tx.NextStmt()
		case cmdListTables:
			tblnames, err := st.ListTables(ctx, state.tx, sql.SchemaName{dbname, scname})
			if err != nil {
				t.Errorf("%sListTables() failed with %s", cmd.fln, err)
			} else {
				var ret []string
				for _, tblname := range tblnames {
					ret = append(ret, tblname.String())
				}
				sort.Strings(ret)
				if !reflect.DeepEqual(cmd.list, ret) {
					t.Errorf("%sListTables() got %v want %v", cmd.fln, ret, cmd.list)
				}
			}
		case cmdRows:
			rows, err := state.tbl.Rows(ctx, nil, nil)
			if err != nil {
				t.Errorf("%stable.Rows() failed with %s", cmd.fln, err)
			} else {
				vals := allRows(t, ctx, rows, cmd.fln)
				if vals != nil {
					testutil.SortValues(vals)
					if !reflect.DeepEqual(vals, cmd.values) {
						t.Errorf("%stable.Rows() got %v want %v", cmd.fln, vals, cmd.values)
					}
				}
			}
		case cmdInsert:
			err := state.tbl.Insert(ctx, cmd.row)
			if err != nil {
				t.Errorf("%stable.Insert() failed with %s", cmd.fln, err)
			}
		case cmdUpdate:
			rows, err := state.tbl.Rows(ctx, nil, nil)
			if err != nil {
				t.Errorf("%stable.Rows() failed with %s", cmd.fln, err)
			} else {
				dest := make([]sql.Value, len(rows.Columns()))
				for {
					err = rows.Next(ctx, dest)
					if err != nil {
						if !cmd.fail {
							t.Errorf("%srows.Next() failed with %s", cmd.fln, err)
						}
						break
					}
					if i64, ok := dest[0].(sql.Int64Value); ok && int(i64) == cmd.rowID {
						err = rows.Update(ctx, cmd.updates)
						if cmd.fail {
							if err == nil {
								t.Errorf("%srows.Update() did not fail", cmd.fln)
							}
						} else if err != nil {
							t.Errorf("%srows.Update() failed with %s", cmd.fln, err)
						}
						break
					}
				}

				err := rows.Close()
				if err != nil {
					t.Errorf("%srows.Close() failed with %s", cmd.fln, err)
				}
			}
		case cmdDelete:
			rows, err := state.tbl.Rows(ctx, nil, nil)
			if err != nil {
				t.Errorf("%stable.Rows() failed with %s", cmd.fln, err)
			} else {
				dest := make([]sql.Value, len(rows.Columns()))
				for {
					err = rows.Next(ctx, dest)
					if err != nil {
						if !cmd.fail {
							t.Errorf("%srows.Next() failed with %s", cmd.fln, err)
						}
						break
					}
					if i64, ok := dest[0].(sql.Int64Value); ok && int(i64) == cmd.rowID {
						err = rows.Delete(ctx)
						if cmd.fail {
							if err == nil {
								t.Errorf("%srows.Delete() did not fail", cmd.fln)
							}
						} else if err != nil {
							t.Errorf("%srows.Delete() failed with %s", cmd.fln, err)
						}
						break
					}
				}

				err := rows.Close()
				if err != nil {
					t.Errorf("%srows.Close() failed with %s", cmd.fln, err)
				}
			}
		default:
			panic("unexpected command")
		}
	}
}

type dbCmd storeCmd

func testStore(t *testing.T, st storage.Store, cmds []dbCmd) {
	for _, cmd := range cmds {
		switch cmd.cmd {
		case cmdCreateDatabase:
			err := st.CreateDatabase(cmd.name, nil)
			if cmd.fail {
				if err == nil {
					t.Errorf("%sCreateDatabase(%s) did not fail", cmd.fln, cmd.name)
				}
			} else if err != nil {
				t.Errorf("%sCreateDatabase(%s) failed with %s", cmd.fln, cmd.name, err)
			}
		case cmdDropDatabase:
			err := st.DropDatabase(cmd.name, cmd.ifExists, nil)
			if cmd.fail {
				if err == nil {
					t.Errorf("%sDropDatabase(%s) did not fail", cmd.fln, cmd.name)
				}
			} else if err != nil {
				t.Errorf("%sDropDatabase(%s) failed with %s", cmd.fln, cmd.name, err)
			}
		default:
			panic("unexpected command")
		}
	}
}

func runTest(t *testing.T, st storage.Store, dbname sql.Identifier, test interface{}) {
	switch tst := test.(type) {
	case string:
		switch tst {
		case "createDatabase":
			err := st.CreateDatabase(dbname, nil)
			if err != nil {
				t.Fatal(err)
			}
		default:
			panic(fmt.Sprintf("unexpected test: %s", tst))
		}
	case []storeCmd:
		testDatabase(t, st, dbname, tst)
	case []dbCmd:
		testStore(t, st, tst)
	default:
		panic(fmt.Sprintf("unexpected test: %#v", tst))
	}
}

var (
	databaseTests = []interface{}{
		[]dbCmd{
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db1")},
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db1"), fail: true},
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db2")},
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db3")},
			{fln: fln(), cmd: cmdDropDatabase, name: sql.ID("dbtest-not-found"), fail: true},
			{fln: fln(), cmd: cmdDropDatabase, name: sql.ID("dbtest-not-found"), ifExists: true},
			{fln: fln(), cmd: cmdDropDatabase, name: sql.ID("dbtest-db1")},
			{fln: fln(), cmd: cmdDropDatabase, name: sql.ID("dbtest-db1"), fail: true},
		},
		[]dbCmd{
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db4")},
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db1")},
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db3"), fail: true},
			{fln: fln(), cmd: cmdDropDatabase, name: sql.ID("dbtest-db3")},
			{fln: fln(), cmd: cmdDropDatabase, name: sql.ID("dbtest-db3"), fail: true},
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db3")},
		},
	}
)

func RunDatabaseTest(t *testing.T, st storage.Store) {
	t.Helper()

	dbname := sql.ID("database_test")
	for _, test := range databaseTests {
		runTest(t, st, dbname, test)
	}
}

var (
	tableTests = []interface{}{
		"createDatabase",
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc-a"), fail: true},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc-a")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc-a")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a"), fail: true},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdCommit},
		},
	}
)

func RunTableTest(t *testing.T, st storage.Store) {
	t.Helper()

	dbname := sql.ID("table_test")
	for _, test := range tableTests {
		runTest(t, st, dbname, test)
	}
}

var (
	tableLifecycleSubtests1 = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1"), fail: true},
		{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl1")},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
		{fln: fln(), cmd: cmdCommit},
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
		{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl1")},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1"), fail: true},
		{fln: fln(), cmd: cmdCommit},
	}
	tableLifecycleSubtests2 = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl4")},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl4")},
		{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl4")},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl4"), fail: true},
		{fln: fln(), cmd: cmdCommit},

		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl4"), fail: true},
		{fln: fln(), cmd: cmdCommit},
	}
	tableLifecycleSubtests3 = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl7")},
		{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl7")},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl7"), fail: true},
		{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl7")},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl7")},
		{fln: fln(), cmd: cmdCommit},
	}

	tableLifecycleTests = []interface{}{
		"createDatabase",
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a"), fail: true},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-a"), fail: true},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-a"}},
			{fln: fln(), cmd: cmdCommit},
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-c")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-d")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-a", "tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-a", "tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-e")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d", "tbl-e"}},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl-c")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-d"}},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		tableLifecycleSubtests1,
		tableLifecycleSubtests1,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl2"), ifExists: true},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl2"), fail: true},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdCommit},
		},
		tableLifecycleSubtests2,
		tableLifecycleSubtests2,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl5")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl5")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl5")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl5")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl5")},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl5")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl6")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl6")},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl6")},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl6"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl7")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl7")},
			{fln: fln(), cmd: cmdCommit},
		},
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl7")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl8")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl8"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl8"), fail: true},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl8")},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl8"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl8"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
	}
)

func RunTableLifecycleTest(t *testing.T, st storage.Store) {
	t.Helper()

	dbname := sql.ID("tbl_lifecycle_test")
	for _, test := range tableLifecycleTests {
		runTest(t, st, dbname, test)
	}
}

var (
	schemaSubtests1 = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc1"), fail: true},
		{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc1")},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc1")},
		{fln: fln(), cmd: cmdCommit},
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc1")},
		{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc1")},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc1"), fail: true},
		{fln: fln(), cmd: cmdCommit},
	}

	schemaSubtests2 = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc4")},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc4")},
		{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc4")},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc4"), fail: true},
		{fln: fln(), cmd: cmdCommit},

		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc4"), fail: true},
		{fln: fln(), cmd: cmdCommit},
	}

	schemaSubtests3 = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc7")},
		{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc7")},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc7"), fail: true},
		{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc7")},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc7")},
		{fln: fln(), cmd: cmdCommit},
	}

	schemaTests = []interface{}{
		"createDatabase",
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc-a"), fail: true},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc-a")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc-a")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc-a")},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc-a"), fail: true},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc-a")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListSchemas, list: []string{"public", "sc-a"}},
			{fln: fln(), cmd: cmdCommit},
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc-b")},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc-c")},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc-d")},
			{fln: fln(), cmd: cmdListSchemas,
				list: []string{"public", "sc-a", "sc-b", "sc-c", "sc-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListSchemas,
				list: []string{"public", "sc-a", "sc-b", "sc-c", "sc-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc-a")},
			{fln: fln(), cmd: cmdListSchemas, list: []string{"public", "sc-b", "sc-c", "sc-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListSchemas, list: []string{"public", "sc-b", "sc-c", "sc-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc-e")},
			{fln: fln(), cmd: cmdListSchemas,
				list: []string{"public", "sc-b", "sc-c", "sc-d", "sc-e"}},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListSchemas, list: []string{"public", "sc-b", "sc-c", "sc-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc-c")},
			{fln: fln(), cmd: cmdListSchemas, list: []string{"public", "sc-b", "sc-d"}},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListSchemas, list: []string{"public", "sc-b", "sc-c", "sc-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdSetSchema, name: sql.ID("sc-z")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl"), fail: true},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		schemaSubtests1,
		schemaSubtests1,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc2"), fail: true},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc2"), ifExists: true},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc2"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc2"), fail: true},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc2"), fail: true},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc2"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc3")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc3")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc3")},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc3")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc3")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc3")},
			{fln: fln(), cmd: cmdCommit},
		},
		schemaSubtests2,
		schemaSubtests2,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc5")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc5")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc5")},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc5")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc5")},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc5")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc6")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc6")},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc6")},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc6"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc7")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc7")},
			{fln: fln(), cmd: cmdCommit},
		},
		schemaSubtests3,
		schemaSubtests3,
		schemaSubtests3,
		schemaSubtests3,
		schemaSubtests3,
		schemaSubtests3,
		schemaSubtests3,
		schemaSubtests3,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc7")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc8")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc8"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc8"), fail: true},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc8")},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc8"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc8"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc9")},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc10")},
			{fln: fln(), cmd: cmdSetSchema, name: sql.ID("sc10")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl1")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl2")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc9")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc10")},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc9")},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc10"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc9"), fail: true},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc10")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdSetSchema, name: sql.ID("sc10")},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl1")},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl2")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc10")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc9"), fail: true},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc10"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
	}
)

func RunSchemaTest(t *testing.T, st storage.Store) {
	t.Helper()

	dbname := sql.ID("schema_test")
	for _, test := range schemaTests {
		runTest(t, st, dbname, test)
	}
}

var (
	tableRowsTests = []interface{}{
		"createDatabase",
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl1")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{fln: fln(), cmd: cmdRows, values: [][]sql.Value{}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{fln: fln(), cmd: cmdRows, values: [][]sql.Value{}},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(1), sql.Int64Value(1),
				sql.StringValue("first row")}},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(2), sql.Int64Value(4),
				sql.StringValue("second row")}},
			{fln: fln(), cmd: cmdNextStmt},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(3), sql.Int64Value(9),
				sql.StringValue("third row")}},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(4), sql.Int64Value(16),
				sql.StringValue("fourth row")}},
			{fln: fln(), cmd: cmdNextStmt},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(3), sql.Int64Value(9),
				sql.StringValue("third row")}},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(4), sql.Int64Value(16),
				sql.StringValue("fourth row")}},
			{fln: fln(), cmd: cmdNextStmt},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl2")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl2")},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(1), sql.Int64Value(1),
				sql.StringValue("first row")}},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(2), sql.Int64Value(4),
				sql.StringValue("second row")}},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(3), sql.Int64Value(9),
				sql.StringValue("third row")}},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(4), sql.Int64Value(16),
				sql.StringValue("fourth row")}},
			{fln: fln(), cmd: cmdNextStmt},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl2")},
			{fln: fln(), cmd: cmdDelete, rowID: 4},
			{fln: fln(), cmd: cmdNextStmt},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl2")},
			{fln: fln(), cmd: cmdDelete, rowID: 1},
			{fln: fln(), cmd: cmdNextStmt},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl2")},
			{fln: fln(), cmd: cmdDelete, rowID: 2},
			{fln: fln(), cmd: cmdNextStmt},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
				},
			},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl2")},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(1), sql.Int64Value(1),
				sql.StringValue("first row")}},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(2), sql.Int64Value(4),
				sql.StringValue("second row")}},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(3), sql.Int64Value(9),
				sql.StringValue("third row")}},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(4), sql.Int64Value(16),
				sql.StringValue("fourth row")}},
			{fln: fln(), cmd: cmdNextStmt},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdUpdate, rowID: 1,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(10)}}},
			{fln: fln(), cmd: cmdNextStmt},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(10), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdUpdate, rowID: 2,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(40)}}},
			{fln: fln(), cmd: cmdNextStmt},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(10), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(40), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(10), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(9), sql.StringValue("third row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdUpdate, rowID: 3,
				updates: []sql.ColumnUpdate{
					{Index: 1, Value: sql.Int64Value(90)},
					{Index: 2, Value: sql.StringValue("3rd row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(10), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
					{sql.Int64Value(3), sql.Int64Value(90), sql.StringValue("3rd row")},
					{sql.Int64Value(4), sql.Int64Value(16), sql.StringValue("fourth row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl4")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl4")},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(1), sql.Int64Value(1),
				sql.StringValue("first row")}},
			{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(2), sql.Int64Value(4),
				sql.StringValue("second row")}},
			{fln: fln(), cmd: cmdNextStmt},
			{fln: fln(), cmd: cmdRows,
				values: [][]sql.Value{
					{sql.Int64Value(1), sql.Int64Value(1), sql.StringValue("first row")},
					{sql.Int64Value(2), sql.Int64Value(4), sql.StringValue("second row")},
				},
			},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl4")},
			{fln: fln(), cmd: cmdDelete, rowID: 1},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl4")},
			{fln: fln(), cmd: cmdUpdate, rowID: 1,
				updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(40)}}},
			{fln: fln(), cmd: cmdCommit},
		},
	}
)

func RunTableRowsTest(t *testing.T, st storage.Store) {
	t.Helper()

	dbname := sql.ID("table_rows_test")
	for _, test := range tableRowsTests {
		runTest(t, st, dbname, test)
	}
}

func RunParallelTest(t *testing.T, st storage.Store) {
	t.Helper()

	dbname := sql.ID("parallel_test")
	err := st.CreateDatabase(sql.ID("parallel_test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	testDatabase(t, st, dbname,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl")},
			{fln: fln(), cmd: cmdCommit},
		})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i, r int) {
			defer wg.Done()

			for j := 0; j < r; j++ {
				testDatabase(t, st, dbname,
					[]storeCmd{
						{fln: fln(), cmd: cmdBegin},
						{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl")},
						{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(i*r + j),
							sql.Int64Value(j), sql.StringValue(fmt.Sprintf("row %d.%d", i, j))}},
						{fln: fln(), cmd: cmdCommit},
					})
			}

			for j := 0; j < r; j++ {
				testDatabase(t, st, dbname,
					[]storeCmd{
						{fln: fln(), cmd: cmdBegin},
						{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl")},
						{fln: fln(), cmd: cmdUpdate, rowID: i*r + j,
							updates: []sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(j * j)}}},
						{fln: fln(), cmd: cmdCommit},
					})
			}
		}(i, 100)
	}
	wg.Wait()
}

func incColumn(t *testing.T, st storage.Store, tx storage.Transaction, tdx uint64, i int,
	tn sql.TableName) bool {

	var ctx context.Context

	tbl, err := st.LookupTable(ctx, tx, tn)
	if err != nil {
		t.Fatalf("LookupTable(%s) failed with %s", tn, err)
	}
	rows, err := tbl.Rows(ctx, nil, nil)
	if err != nil {
		t.Fatalf("table.Rows() failed with %s", err)
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			t.Errorf("rows.Close() failed with %s", err)
		}
	}()

	dest := make([]sql.Value, len(rows.Columns()))
	for {
		err = rows.Next(ctx, dest)
		if err != nil {
			if err != io.EOF {
				t.Errorf("rows.Next() failed with %s", err)
			}
			t.Fatalf("rows.Next() row not found")
		}
		if i64, ok := dest[0].(sql.Int64Value); ok && int(i64) == i {
			v := int(dest[1].(sql.Int64Value))
			err = rows.Update(ctx,
				[]sql.ColumnUpdate{{Index: 1, Value: sql.Int64Value(v + 1)}})
			if err == nil {
				//fmt.Printf("%d: %d -> %d\n", tdx, i, v+1)
				return true
			}
			break
		}
	}

	return false
}

func RunStressTest(t *testing.T, st storage.Store) {
	t.Helper()

	dbname := sql.ID("stress_test")
	err := st.CreateDatabase(dbname, nil)
	if err != nil {
		t.Fatal(err)
	}

	testDatabase(t, st, dbname,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl")},
			{fln: fln(), cmd: cmdCommit},
		})

	const rcnt = 100

	for i := 0; i < rcnt; i++ {
		testDatabase(t, st, dbname,
			[]storeCmd{
				{fln: fln(), cmd: cmdBegin},
				{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl")},
				{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(i),
					sql.Int64Value(0), sql.StringValue(fmt.Sprintf("row %d", i))}},
				{fln: fln(), cmd: cmdCommit},
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
			i := 0
			for i < r {
				updated := false
				for !updated {
					tx := st.Begin(tdx)
					updated = incColumn(t, st, tx, tdx, i, sql.TableName{dbname, sql.PUBLIC, name})
					if updated {
						err := tx.Commit(ctx)
						if err == nil {
							i += 1
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

	testDatabase(t, st, dbname,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl")},
			{fln: fln(), cmd: cmdRows, values: values},
			{fln: fln(), cmd: cmdCommit},
		})
}
