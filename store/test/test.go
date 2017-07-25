package test

import (
	"fmt"
	"io"
	"maho/sql"
	"maho/store"
)

type testStore struct{}

type testDatabase struct {
	name   sql.Identifier
	tables map[sql.Identifier]*testTable
}

type testTable struct {
	name      sql.Identifier
	columns   []sql.Column
	columnMap store.ColumnMap
	rows      [][]sql.Value
}

type AllRows interface {
	AllRows() [][]sql.Value
}

type testRows struct {
	columns []sql.Column
	rows    [][]sql.Value
	index   int
}

func (ts testStore) Open(name string) (store.Database, error) {
	var tdb testDatabase
	tdb.name = sql.ID(name)
	tdb.tables = make(map[sql.Identifier]*testTable)
	return &tdb, nil
}

func init() {
	store.Register("test", testStore{})
}

func (tdb *testDatabase) Name() sql.Identifier {
	return tdb.name
}

func (tdb *testDatabase) Type() sql.Identifier {
	return sql.ID("test")
}

func (tdb *testDatabase) CreateTable(name sql.Identifier, cols []sql.Column) error {
	if _, ok := tdb.tables[name]; ok {
		return fmt.Errorf("test: table \"%s\" already exists in database \"%s\"", name, tdb.name)
	}
	cmap := make(store.ColumnMap)
	for i, c := range cols {
		cmap[c.Name] = i
	}
	tbl := testTable{name, cols, cmap, nil}
	tdb.tables[name] = &tbl
	return nil
}

func (tdb *testDatabase) DropTable(name sql.Identifier) error {
	if _, ok := tdb.tables[name]; !ok {
		return fmt.Errorf("test: table \"%s\" does not exist in database \"%s\"", name, tdb.name)
	}
	delete(tdb.tables, name)
	return nil
}

func (tdb *testDatabase) Table(name sql.Identifier) (store.Table, error) {
	tbl, ok := tdb.tables[name]
	if !ok {
		return nil, fmt.Errorf("test: table \"%s\" not found in database \"%s\"", name, tdb.name)
	}
	return tbl, nil
}

func (tdb *testDatabase) Tables() ([]sql.Identifier, [][]sql.Column) {
	names := make([]sql.Identifier, len(tdb.tables))
	cols := make([][]sql.Column, len(tdb.tables))
	i := 0
	for _, tbl := range tdb.tables {
		names[i] = tbl.name
		cols[i] = make([]sql.Column, len(tbl.columns))
		copy(cols[i], tbl.columns)
		i += 1
	}
	return names, cols
}

func (tt *testTable) Name() sql.Identifier {
	return tt.name
}

func (tt *testTable) Columns() []sql.Column {
	return tt.columns
}

func (tt *testTable) ColumnMap() store.ColumnMap {
	return tt.columnMap
}

func (tt *testTable) Rows() (store.Rows, error) {
	return &testRows{columns: tt.columns, rows: tt.rows}, nil
}

func (tt *testTable) Insert(row []sql.Value) error {
	tt.rows = append(tt.rows, row)
	return nil
}

func (tt *testTable) AllRows() [][]sql.Value {
	return tt.rows
}

func (tr *testRows) Columns() []sql.Column {
	return tr.columns
}

func (tr *testRows) Close() error {
	tr.index = len(tr.rows)
	return nil
}

func (tr *testRows) Next(dest []sql.Value) error {
	if tr.index == len(tr.rows) {
		return io.EOF
	}
	copy(dest, tr.rows[tr.index])
	tr.index += 1
	return nil
}
