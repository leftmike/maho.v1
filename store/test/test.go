package test

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/store"
)

type TestStore struct {
	Typ string
}

type testDatabase struct {
	name   sql.Identifier
	typ    sql.Identifier
	tables map[sql.Identifier]*testTable
}

type testTable struct {
	name        sql.Identifier
	columns     []sql.Identifier
	columnTypes []db.ColumnType
	rows        [][]sql.Value
}

type testRows struct {
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
	haveRow bool
}

func (ts TestStore) Open(name string) (db.Database, error) {
	var tdb testDatabase
	tdb.name = sql.ID(name)
	tdb.typ = sql.ID(ts.Typ)
	tdb.tables = make(map[sql.Identifier]*testTable)
	return &tdb, nil
}

func init() {
	store.Register("test", TestStore{"test"})
}

func (tdb *testDatabase) Name() sql.Identifier {
	return tdb.name
}

func (tdb *testDatabase) Type() sql.Identifier {
	return tdb.typ
}

func (tdb *testDatabase) CreateTable(name sql.Identifier, cols []sql.Identifier,
	colTypes []db.ColumnType) error {

	if _, ok := tdb.tables[name]; ok {
		return fmt.Errorf("test: table \"%s\" already exists in database \"%s\"", name, tdb.name)
	}

	tbl := testTable{name, cols, colTypes, nil}
	tdb.tables[name] = &tbl
	return nil
}

func (tdb *testDatabase) DropTable(name sql.Identifier, exists bool) error {
	if _, ok := tdb.tables[name]; !ok {
		if exists {
			return nil
		}
		return fmt.Errorf("test: table \"%s\" does not exist in database \"%s\"", name, tdb.name)
	}
	delete(tdb.tables, name)
	return nil
}

func (tdb *testDatabase) Table(name sql.Identifier) (db.Table, error) {
	tbl, ok := tdb.tables[name]
	if !ok {
		return nil, fmt.Errorf("test: table \"%s\" not found in database \"%s\"", name, tdb.name)
	}
	return tbl, nil
}

func (tdb *testDatabase) Tables() []sql.Identifier {
	names := make([]sql.Identifier, len(tdb.tables))
	i := 0
	for _, tbl := range tdb.tables {
		names[i] = tbl.name
		i += 1
	}
	return names
}

func (tt *testTable) Name() sql.Identifier {
	return tt.name
}

func (tt *testTable) Columns() []sql.Identifier {
	return tt.columns
}

func (tt *testTable) ColumnTypes() []db.ColumnType {
	return tt.columnTypes
}

func (tt *testTable) Rows() (db.Rows, error) {
	return &testRows{columns: tt.columns, rows: tt.rows}, nil
}

func (tt *testTable) DeleteRows() (db.DeleteRows, error) {
	return &testRows{columns: tt.columns, rows: tt.rows}, nil
}

func (tt *testTable) Insert(row []sql.Value) error {
	tt.rows = append(tt.rows, row)
	return nil
}

func (tr *testRows) Columns() []sql.Identifier {
	return tr.columns
}

func (tr *testRows) Close() error {
	tr.index = len(tr.rows)
	tr.haveRow = false
	return nil
}

func (tr *testRows) Next(dest []sql.Value) error {
	for tr.index < len(tr.rows) {
		if tr.rows[tr.index] != nil {
			copy(dest, tr.rows[tr.index])
			tr.index += 1
			tr.haveRow = true
			return nil
		}
		tr.index += 1
	}

	tr.haveRow = false
	return io.EOF
}

func (tr *testRows) Delete() error {
	if !tr.haveRow {
		return fmt.Errorf("test: no row to delete")
	}
	tr.haveRow = false
	tr.rows[tr.index-1] = nil
	return nil
}
