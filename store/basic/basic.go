package basic

import (
	"fmt"
	"maho/sql"
	"maho/store"
)

type basicStore struct {
	name   string
	tables map[sql.Identifier]*basicTable
}

type basicTable struct {
	name    sql.Identifier
	columns []sql.Column
}

func Make(name string) (store.Store, error) {
	var bs basicStore
	bs.name = name
	bs.tables = make(map[sql.Identifier]*basicTable)
	return &bs, nil
}

func (bs *basicStore) Type() sql.Identifier {
	return sql.BASIC
}

func (bs *basicStore) CreateTable(name sql.Identifier, cols []sql.Column) error {
	if _, ok := bs.tables[name]; ok {
		return fmt.Errorf("basic: table \"%s\" already exists in database \"%s\"", name, bs.name)
	}
	tbl := basicTable{name, cols}
	bs.tables[name] = &tbl
	return nil
}

func (bs *basicStore) Table(name sql.Identifier) (store.Table, error) {
	tbl, ok := bs.tables[name]
	if !ok {
		return nil, fmt.Errorf("basic: table \"%s\" not found in database \"%s\"", name, bs.name)
	}
	return tbl, nil
}

func (bs *basicStore) Tables() ([]sql.Identifier, [][]sql.Column) {
	names := make([]sql.Identifier, len(bs.tables))
	cols := make([][]sql.Column, len(bs.tables))
	i := 0
	for _, tbl := range bs.tables {
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

func (bt *basicTable) Rows() (store.Rows, error) {
	return nil, fmt.Errorf("basic: not implemented")
}
