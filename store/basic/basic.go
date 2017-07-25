package basic

import (
	"fmt"
	"io"
	"maho/sql"
	"maho/store"
)

type basicStore struct{}

type basicDatabase struct {
	name   sql.Identifier
	tables map[sql.Identifier]*basicTable
}

type basicTable struct {
	name      sql.Identifier
	columns   []sql.Column
	columnMap store.ColumnMap
	rows      [][]sql.Value
}

type basicRows struct {
	columns []sql.Column
	rows    [][]sql.Value
	index   int
}

func (bs basicStore) Open(name string) (store.Database, error) {
	var bdb basicDatabase
	bdb.name = sql.ID(name)
	bdb.tables = make(map[sql.Identifier]*basicTable)
	return &bdb, nil
}

func init() {
	store.Register("basic", basicStore{})
}

func (bdb *basicDatabase) Name() sql.Identifier {
	return bdb.name
}

func (bdb *basicDatabase) Type() sql.Identifier {
	return sql.BASIC
}

func (bdb *basicDatabase) CreateTable(name sql.Identifier, cols []sql.Column) error {
	if _, ok := bdb.tables[name]; ok {
		return fmt.Errorf("basic: table \"%s\" already exists in database \"%s\"", name, bdb.name)
	}
	cmap := make(store.ColumnMap)
	for i, c := range cols {
		cmap[c.Name] = i
	}
	tbl := basicTable{name, cols, cmap, nil}
	bdb.tables[name] = &tbl
	return nil
}

func (bdb *basicDatabase) DropTable(name sql.Identifier) error {
	if _, ok := bdb.tables[name]; !ok {
		return fmt.Errorf("basic: table \"%s\" does not exist in database \"%s\"", name, bdb.name)
	}
	delete(bdb.tables, name)
	return nil
}

func (bdb *basicDatabase) Table(name sql.Identifier) (store.Table, error) {
	tbl, ok := bdb.tables[name]
	if !ok {
		return nil, fmt.Errorf("basic: table \"%s\" not found in database \"%s\"", name, bdb.name)
	}
	return tbl, nil
}

func (bdb *basicDatabase) Tables() ([]sql.Identifier, [][]sql.Column) {
	names := make([]sql.Identifier, len(bdb.tables))
	cols := make([][]sql.Column, len(bdb.tables))
	i := 0
	for _, tbl := range bdb.tables {
		names[i] = tbl.name
		cols[i] = make([]sql.Column, len(tbl.columns))
		copy(cols[i], tbl.columns)
		i += 1
	}
	return names, cols
}

func (bt *basicTable) Name() sql.Identifier {
	return bt.name
}

func (bt *basicTable) Columns() []sql.Column {
	return bt.columns
}

func (bt *basicTable) ColumnMap() store.ColumnMap {
	return bt.columnMap
}

func (bt *basicTable) Rows() (store.Rows, error) {
	return &basicRows{columns: bt.columns, rows: bt.rows}, nil
}

func (bt *basicTable) Insert(row []sql.Value) error {
	bt.rows = append(bt.rows, row)
	return nil
}

func (br *basicRows) Columns() []sql.Column {
	return br.columns
}

func (br *basicRows) Close() error {
	br.index = len(br.rows)
	return nil
}

func (br *basicRows) Next(dest []sql.Value) error {
	if br.index == len(br.rows) {
		return io.EOF
	}
	copy(dest, br.rows[br.index])
	br.index += 1
	return nil
}
