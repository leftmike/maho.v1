package basic

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type basicEngine struct {
	databases map[string]*basicDatabase
}

type basicDatabase struct {
	tables map[sql.Identifier]*basicTable
}

type basicTable struct {
	name        sql.Identifier
	columns     []sql.Identifier
	columnTypes []db.ColumnType
	rows        [][]sql.Value
}

type basicRows struct {
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
	haveRow bool
}

func init() {
	engine.Register("basic", &basicEngine{})
}

func (be *basicEngine) Start(dir string) error {
	be.databases = map[string]*basicDatabase{}
	return nil
}

func (be *basicEngine) CreateDatabase(dbname string) error {
	if _, dup := be.databases[dbname]; dup {
		return fmt.Errorf("basic: database %s already exists", dbname)
	}
	be.databases[dbname] = &basicDatabase{map[sql.Identifier]*basicTable{}}
	return nil
}

func (be *basicEngine) OpenDatabase(dbname string) (bool, error) {
	_, ok := be.databases[dbname]
	if ok {
		return true, nil
	}
	return false, nil
}

func (be *basicEngine) LookupTable(dbname string, tblname sql.Identifier) (db.Table, error) {
	bdb, ok := be.databases[dbname]
	if !ok {
		return nil, fmt.Errorf("basic: database %s not found", dbname)
	}
	tbl, ok := bdb.tables[tblname]
	if !ok {
		return nil, fmt.Errorf("basic: table %s not found in database %s", tblname, dbname)
	}
	return tbl, nil
}

func (be *basicEngine) CreateTable(dbname string, tblname sql.Identifier, cols []sql.Identifier,
	colTypes []db.ColumnType) error {

	bdb, ok := be.databases[dbname]
	if !ok {
		return fmt.Errorf("basic: database %s not found", dbname)
	}
	if _, dup := bdb.tables[tblname]; dup {
		return fmt.Errorf("basic: table %s already exists in database %s", tblname, dbname)
	}

	bdb.tables[tblname] = &basicTable{tblname, cols, colTypes, nil}
	return nil
}

func (be *basicEngine) DropTable(dbname string, tblname sql.Identifier, exists bool) error {
	bdb, ok := be.databases[dbname]
	if !ok {
		return fmt.Errorf("basic: database %s not found", dbname)
	}
	if _, ok := bdb.tables[tblname]; !ok {
		if exists {
			return nil
		}
		return fmt.Errorf("basic: table %s does not exist in database %s", tblname, dbname)
	}
	delete(bdb.tables, tblname)
	return nil
}

func (tt *basicTable) Name() sql.Identifier {
	return tt.name
}

func (tt *basicTable) Columns() []sql.Identifier {
	return tt.columns
}

func (tt *basicTable) ColumnTypes() []db.ColumnType {
	return tt.columnTypes
}

func (tt *basicTable) Rows() (db.Rows, error) {
	return &basicRows{columns: tt.columns, rows: tt.rows}, nil
}

func (tt *basicTable) DeleteRows() (db.DeleteRows, error) {
	return &basicRows{columns: tt.columns, rows: tt.rows}, nil
}

func (tt *basicTable) UpdateRows() (db.UpdateRows, error) {
	return &basicRows{columns: tt.columns, rows: tt.rows}, nil
}

func (tt *basicTable) Insert(row []sql.Value) error {
	tt.rows = append(tt.rows, row)
	return nil
}

func (tr *basicRows) Columns() []sql.Identifier {
	return tr.columns
}

func (tr *basicRows) Close() error {
	tr.index = len(tr.rows)
	tr.haveRow = false
	return nil
}

func (tr *basicRows) Next(dest []sql.Value) error {
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

func (tr *basicRows) Delete() error {
	if !tr.haveRow {
		return fmt.Errorf("basic: no row to delete")
	}
	tr.haveRow = false
	tr.rows[tr.index-1] = nil
	return nil
}

func (tr *basicRows) Update(updates []db.ColumnUpdate) error {
	if !tr.haveRow {
		return fmt.Errorf("basic: no row to update")
	}
	row := tr.rows[tr.index-1]
	for _, up := range updates {
		row[up.Index] = up.Value
	}
	return nil
}
