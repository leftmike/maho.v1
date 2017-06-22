package test

import (
	"fmt"
	"io"
	"maho/sql"
	"maho/store"
)

type testStore struct {
	name   sql.Identifier
	tables map[sql.Identifier]*testTable
}

type testTable struct {
	name      sql.Identifier
	columns   []sql.Column
	columnMap store.ColumnMap
	rows      [][]sql.Value
}

type testRows struct {
	columns []sql.Column
	rows    [][]sql.Value
	index   int
}

func Make(id sql.Identifier) (store.Store, error) {
	var ts testStore
	ts.name = id
	ts.tables = make(map[sql.Identifier]*testTable)
	return &ts, nil
}

func (ts *testStore) Name() sql.Identifier {
	return ts.name
}

func (ts *testStore) Type() sql.Identifier {
	return sql.Id("test")
}

func (ts *testStore) CreateTable(name sql.Identifier, cols []sql.Column) error {
	if _, ok := ts.tables[name]; ok {
		return fmt.Errorf("test: table \"%s\" already exists in database \"%s\"", name, ts.name)
	}
	cmap := make(store.ColumnMap)
	for i, c := range cols {
		cmap[c.Name] = i
	}
	tbl := testTable{name, cols, cmap, nil}
	ts.tables[name] = &tbl
	return nil
}

func (ts *testStore) Table(name sql.Identifier) (store.Table, error) {
	tbl, ok := ts.tables[name]
	if !ok {
		return nil, fmt.Errorf("test: table \"%s\" not found in database \"%s\"", name, ts.name)
	}
	return tbl, nil
}

func (ts *testStore) Tables() ([]sql.Identifier, [][]sql.Column) {
	names := make([]sql.Identifier, len(ts.tables))
	cols := make([][]sql.Column, len(ts.tables))
	i := 0
	for _, tbl := range ts.tables {
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
