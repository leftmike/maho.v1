package test

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sort"
	"testing"

	"github.com/leftmike/maho/engine"
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
	cmdAddIndex
	cmdRemoveIndex
	cmdIndexRows
	cmdIndexDelete
	cmdIndexUpdate
	cmdCreateDatabase
	cmdDropDatabase
)

func fln() testutil.FileLineNumber {
	return testutil.MakeFileLineNumber()
}

type storeCmd struct {
	fln       testutil.FileLineNumber
	cmd       int
	tdx       int                // Which transaction to use
	fail      bool               // The command should fail
	ifExists  bool               // Flag for DropTable or DropSchema
	name      sql.Identifier     // Name of the table or schema
	list      []string           // List of table names or schema names
	values    [][]sql.Value      // Expected rows (table.Rows, table.IndexRows)
	idxValues [][]sql.Value      // Expected index rows (table.IndexRows)
	row       []sql.Value        // Row to insert (table.Insert)
	rowID     int                // Row to update (rows.Update) or delete (rows.Delete)
	updates   []sql.ColumnUpdate // Updates to a row (rows.Update)
	idxname   sql.Identifier     // AddIndex and RemoveIndex
	key       []sql.ColumnKey    // Primary or index key
	unique    bool               // Unique index
	check     bool
	cols      []sql.Identifier
	colTypes  []sql.ColumnType
}

type transactionState struct {
	tdx int
	tx  engine.Transaction
	tbl engine.Table
	tt  *engine.TableType
}

var (
	columns     = []sql.Identifier{sql.ID("ID"), sql.ID("intCol"), sql.ID("stringCol")}
	columnTypes = []sql.ColumnType{int32ColType, int64ColType, stringColType}
	primary     = []sql.ColumnKey{sql.MakeColumnKey(0, false)}
)

func allRows(t *testing.T, ctx context.Context, rows engine.Rows,
	fln testutil.FileLineNumber) [][]sql.Value {

	t.Helper()

	var all [][]sql.Value
	for {
		dest, err := rows.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Errorf("%sRows.Next(): failed with %s", fln, err)
			return nil
		}
		if len(all) > 0 && len(all[0]) != len(dest) {
			fmt.Printf("%slen: %d len(dest): %d\n", fln, len(all[0]), len(dest))
		}
		all = append(all, append(make([]sql.Value, 0, len(dest)), dest...))
	}

	err := rows.Close()
	if err != nil {
		t.Errorf("%sRows.Close(): failed with %s", fln, err)
	}
	return all
}

func allIndexRows(t *testing.T, ctx context.Context, idxRows engine.IndexRows,
	fln testutil.FileLineNumber) ([][]sql.Value, [][]sql.Value) {

	t.Helper()

	var allIdx, all [][]sql.Value
	for {
		dest, err := idxRows.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Errorf("%sIndexRows.Next(): failed with %s", fln, err)
			return nil, nil
		}
		if len(allIdx) > 0 && len(allIdx[0]) != len(dest) {
			fmt.Printf("%slen: %d len(dest): %d\n", fln, len(allIdx[0]), len(dest))
		}
		allIdx = append(allIdx, append(make([]sql.Value, 0, len(dest)), dest...))

		dest, err = idxRows.Row(ctx)
		if err != nil {
			t.Errorf("%sidxIndexRows.Row(): failed with %s", fln, err)
			return nil, nil
		}
		if len(all) > 0 && len(all[0]) != len(dest) {
			fmt.Printf("%slen: %d len(dest): %d\n", fln, len(all[0]), len(dest))
		}
		all = append(all, append(make([]sql.Value, 0, len(dest)), dest...))
	}

	err := idxRows.Close()
	if err != nil {
		t.Errorf("%sIndexRows.Close(): failed with %s", fln, err)
	}
	return allIdx, all
}

func rowUpdate(ctx context.Context, rows engine.Rows, updates []sql.ColumnUpdate,
	curRow []sql.Value) error {

	updateRow := append(make([]sql.Value, 0, len(curRow)), curRow...)
	updatedCols := make([]int, 0, len(updates))
	for _, update := range updates {
		updateRow[update.Column] = update.Value
		updatedCols = append(updatedCols, update.Column)
	}

	return rows.Update(ctx, updatedCols, updateRow)
}

func indexRowUpdate(ctx context.Context, idxRows engine.IndexRows, updates []sql.ColumnUpdate,
	curRow []sql.Value) error {

	updateRow := append(make([]sql.Value, 0, len(curRow)), curRow...)
	updatedCols := make([]int, 0, len(updates))
	for _, update := range updates {
		updateRow[update.Column] = update.Value
		updatedCols = append(updatedCols, update.Column)
	}

	return idxRows.Update(ctx, updatedCols, updateRow)
}

func testDatabase(t *testing.T, st *storage.Store, dbname sql.Identifier, cmds []storeCmd) {
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
		case cmdIndexRows:
		case cmdIndexDelete:
		case cmdIndexUpdate:
		default:
			state.tbl = nil
			state.tt = nil
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
			state.tbl, state.tt, err = st.LookupTable(ctx, state.tx,
				sql.TableName{dbname, scname, cmd.name})
			if cmd.fail {
				if err == nil {
					t.Errorf("%sLookupTable(%s) did not fail", cmd.fln, cmd.name)
				}
			} else if err != nil {
				t.Errorf("%sLookupTable(%s) failed with %s", cmd.fln, cmd.name, err)
			} else if cmd.check {
				cols := state.tt.Columns()
				if !reflect.DeepEqual(cols, columns) {
					t.Errorf("%stbl.Columns() got %v want %v", cmd.fln, cols, columns)
				}
				colTypes := state.tt.ColumnTypes()
				if !reflect.DeepEqual(colTypes, columnTypes) {
					t.Errorf("%stbl.ColumnTypes() got %v want %v", cmd.fln, colTypes, columnTypes)
				}
			}
		case cmdCreateTable:
			var tt *engine.TableType
			if cmd.cols == nil {
				tt = engine.MakeTableType(columns, columnTypes, primary)
			} else {
				tt = engine.MakeTableType(cmd.cols, cmd.colTypes, cmd.key)
			}
			err := st.CreateTable(ctx, state.tx, sql.TableName{dbname, scname, cmd.name}, tt,
				false)
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
					if !reflect.DeepEqual(vals, cmd.values) {
						t.Errorf("%stable.Rows() got %v want %v", cmd.fln, vals, cmd.values)
					}
				}
			}
		case cmdInsert:
			err := state.tbl.Insert(ctx, cmd.row)
			if cmd.fail {
				if err == nil {
					t.Errorf("%stable.Insert() did not fail", cmd.fln)
				}
			} else if err != nil {
				t.Errorf("%stable.Insert() failed with %s", cmd.fln, err)
			}
		case cmdUpdate:
			rows, err := state.tbl.Rows(ctx, nil, nil)
			if err != nil {
				t.Errorf("%stable.Rows() failed with %s", cmd.fln, err)
			} else {
				for {
					dest, err := rows.Next(ctx)
					if err != nil {
						if !cmd.fail {
							t.Errorf("%srows.Next() failed with %s", cmd.fln, err)
						}
						break
					}
					if i64, ok := dest[0].(sql.Int64Value); ok && int(i64) == cmd.rowID {
						err = rowUpdate(ctx, rows, cmd.updates, dest)
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
				for {
					dest, err := rows.Next(ctx)
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
		case cmdAddIndex:
			tn := sql.TableName{dbname, scname, cmd.name}
			tt, err := st.LookupTableType(ctx, state.tx, tn)
			if err != nil {
				t.Errorf("%sLookupTableType(%s) failed with %s", cmd.fln, cmd.name, err)
			} else {
				tt, it, found := tt.AddIndex(cmd.idxname, cmd.unique, cmd.key)
				if cmd.fail {
					if !found {
						t.Errorf("%sAddIndex(%s) index %s did not fail", cmd.fln, cmd.name,
							cmd.idxname)
					}
				} else if found {
					t.Errorf("%sAddIndex(%s) index %s already exists", cmd.fln, cmd.name,
						cmd.idxname)

				} else {
					err = st.AddIndex(ctx, state.tx, tn, tt, it)
					if err != nil {
						t.Errorf("%sAddIndex(%s, %s) failed with %s", cmd.fln, cmd.name, cmd.idxname,
							err)
					}
				}
			}
		case cmdRemoveIndex:
			tn := sql.TableName{dbname, scname, cmd.name}
			tt, err := st.LookupTableType(ctx, state.tx, tn)
			if err != nil {
				t.Errorf("%sLookupTableType(%s) failed with %s", cmd.fln, cmd.name, err)
			} else {
				tt, rdx := tt.RemoveIndex(cmd.idxname)
				if cmd.fail {
					if tt != nil {
						t.Errorf("%sRemoveIndex(%s) index %s did not fail", cmd.fln, cmd.name,
							cmd.idxname)
					}
				} else if tt == nil {
					t.Errorf("%sRemoveIndex(%s) index %s not found", cmd.fln, cmd.name,
						cmd.idxname)
				} else {
					err = st.RemoveIndex(ctx, state.tx, tn, tt, rdx)
					if err != nil {
						t.Errorf("%sRemoveIndex(%s, %s) failed with %s", cmd.fln, cmd.name,
							cmd.idxname, err)
					}
				}
			}
		case cmdIndexRows:
			rdx := -1
			for idx, it := range state.tt.Indexes() {
				if it.Name == cmd.idxname {
					rdx = idx
					break
				}
			}
			if rdx == -1 {
				t.Errorf("%sIndexRows(%s): index %s not found", cmd.fln, cmd.name, cmd.idxname)
				continue
			}

			idxRows, err := state.tbl.IndexRows(ctx, rdx, nil, nil)
			if err != nil {
				t.Errorf("%stable.IndexRows() failed with %s", cmd.fln, err)
			} else {
				idxVals, vals := allIndexRows(t, ctx, idxRows, cmd.fln)
				if idxVals != nil {
					if !reflect.DeepEqual(idxVals, cmd.idxValues) {
						t.Errorf("%stable.IndexRows() got %v want %v", cmd.fln, idxVals,
							cmd.idxValues)
					}
				}
				if vals != nil {
					if !reflect.DeepEqual(vals, cmd.values) {
						t.Errorf("%stable.IndexRows() got %v want %v", cmd.fln, vals, cmd.values)
					}
				}
			}
		case cmdIndexDelete:
			rdx := -1
			for idx, it := range state.tt.Indexes() {
				if it.Name == cmd.idxname {
					rdx = idx
					break
				}
			}
			if rdx == -1 {
				t.Errorf("%sIndexRows(%s): index %s not found", cmd.fln, cmd.name, cmd.idxname)
				continue
			}

			idxRows, err := state.tbl.IndexRows(ctx, rdx, nil, nil)
			if err != nil {
				t.Errorf("%stable.IndexRows() failed with %s", cmd.fln, err)
				continue
			}

			for {
				_, err := idxRows.Next(ctx)
				if err != nil {
					if !cmd.fail {
						t.Errorf("%sIndexRows.Next() failed with %s", cmd.fln, err)
					}
					break
				}
				dest, err := idxRows.Row(ctx)
				if err != nil {
					t.Errorf("%sIndexRows.Row() failed with %s", cmd.fln, err)
				}

				if i64, ok := dest[0].(sql.Int64Value); ok && int(i64) == cmd.rowID {
					err = idxRows.Delete(ctx)
					if cmd.fail {
						if err == nil {
							t.Errorf("%sIndexRows.Delete() did not fail", cmd.fln)
						}
					} else if err != nil {
						t.Errorf("%sIndexRows.Delete() failed with %s", cmd.fln, err)
					}
					break
				}
			}

			err = idxRows.Close()
			if err != nil {
				t.Errorf("%sIndexRows.Close() failed with %s", cmd.fln, err)
			}
		case cmdIndexUpdate:
			rdx := -1
			for idx, it := range state.tt.Indexes() {
				if it.Name == cmd.idxname {
					rdx = idx
					break
				}
			}
			if rdx == -1 {
				t.Errorf("%sIndexRows(%s): index %s not found", cmd.fln, cmd.name, cmd.idxname)
				continue
			}

			idxRows, err := state.tbl.IndexRows(ctx, rdx, nil, nil)
			if err != nil {
				t.Errorf("%stable.IndexRows() failed with %s", cmd.fln, err)
				continue
			}

			for {
				_, err := idxRows.Next(ctx)
				if err != nil {
					if !cmd.fail {
						t.Errorf("%sIndexRows.Next() failed with %s", cmd.fln, err)
					}
					break
				}
				dest, err := idxRows.Row(ctx)
				if err != nil {
					t.Errorf("%sIndexRows.Row() failed with %s", cmd.fln, err)
					break
				}

				if i64, ok := dest[0].(sql.Int64Value); ok && int(i64) == cmd.rowID {
					err = indexRowUpdate(ctx, idxRows, cmd.updates, dest)
					if cmd.fail {
						if err == nil {
							t.Errorf("%sIndexRows.Update() did not fail", cmd.fln)
						}
					} else if err != nil {
						t.Errorf("%sIndexRows.Update() failed with %s", cmd.fln, err)
					}
					break
				}
			}

			err = idxRows.Close()
			if err != nil {
				t.Errorf("%sIndexRows.Close() failed with %s", cmd.fln, err)
			}
		default:
			panic("unexpected command")
		}
	}
}

type dbCmd storeCmd

func testStore(t *testing.T, st *storage.Store, cmds []dbCmd) {
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

func runTest(t *testing.T, st *storage.Store, dbname sql.Identifier, test interface{}) {
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

func i64Val(i int) sql.Value    { return sql.Int64Value(i) }
func strVal(s string) sql.Value { return sql.StringValue(s) }
