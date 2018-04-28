package basic

import (
	"context"
	"fmt"
	"io"
	"math/rand"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type basic struct {
	databases map[sql.Identifier]*database
}

type transaction struct {
	e *basic
}

type database struct {
	nextID engine.TableID
	tables map[sql.Identifier]*table
}

type table struct {
	id          engine.TableID
	pageNum     engine.PageNum
	columns     []sql.Identifier
	columnTypes []db.ColumnType
	rows        [][]sql.Value
}

type rows struct {
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
	haveRow bool
}

func init() {
	engine.Register("basic", &basic{})
}

func (be *basic) Start(dir string) error {
	be.databases = map[sql.Identifier]*database{}
	return nil
}

func (be *basic) CreateDatabase(dbname sql.Identifier) error {
	if _, dup := be.databases[dbname]; dup {
		return fmt.Errorf("basic: database %s already exists", dbname)
	}
	be.databases[dbname] = &database{
		nextID: 1,
		tables: map[sql.Identifier]*table{},
	}
	return nil
}

func (be *basic) ListDatabases() []sql.Identifier {
	var ids []sql.Identifier
	for id := range be.databases {
		ids = append(ids, id)
	}
	return ids
}

func (be *basic) Begin() (engine.Transaction, error) {
	return &transaction{be}, nil
}

func (btx *transaction) LookupTable(ctx context.Context, dbname,
	tblname sql.Identifier) (db.Table, error) {

	bdb, ok := btx.e.databases[dbname]
	if !ok {
		return nil, fmt.Errorf("basic: database %s not found", dbname)
	}
	tbl, ok := bdb.tables[tblname]
	if !ok {
		return nil, fmt.Errorf("basic: table %s not found in database %s", tblname, dbname)
	}
	return tbl, nil
}

func (btx *transaction) CreateTable(ctx context.Context, dbname, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	bdb, ok := btx.e.databases[dbname]
	if !ok {
		return fmt.Errorf("basic: database %s not found", dbname)
	}
	if _, dup := bdb.tables[tblname]; dup {
		return fmt.Errorf("basic: table %s already exists in database %s", tblname, dbname)
	}

	bdb.tables[tblname] = &table{
		id:          bdb.nextID,
		pageNum:     engine.PageNum(rand.Uint64()),
		columns:     cols,
		columnTypes: colTypes,
		rows:        nil,
	}
	bdb.nextID += 1
	return nil
}

func (btx *transaction) DropTable(ctx context.Context, dbname, tblname sql.Identifier,
	exists bool) error {

	bdb, ok := btx.e.databases[dbname]
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

func (btx *transaction) ListTables(ctx context.Context,
	dbname sql.Identifier) ([]engine.TableEntry, error) {

	bdb, ok := btx.e.databases[dbname]
	if !ok {
		return nil, fmt.Errorf("basic: database %s not found", dbname)
	}
	var tbls []engine.TableEntry
	for name, tbl := range bdb.tables {
		tbls = append(tbls, engine.TableEntry{
			Name:    name,
			ID:      tbl.id,
			PageNum: tbl.pageNum,
			Type:    engine.VirtualType,
		})
	}
	return tbls, nil
}

func (btx *transaction) Commit(ctx context.Context) error {
	btx.e = nil
	return nil
}

func (btx *transaction) Rollback() error {
	btx.e = nil
	return nil
}

func (bt *table) Columns() []sql.Identifier {
	return bt.columns
}

func (bt *table) ColumnTypes() []db.ColumnType {
	return bt.columnTypes
}

func (bt *table) Rows() (db.Rows, error) {
	return &rows{columns: bt.columns, rows: bt.rows}, nil
}

func (bt *table) Insert(row []sql.Value) error {
	bt.rows = append(bt.rows, row)
	return nil
}

func (br *rows) Columns() []sql.Identifier {
	return br.columns
}

func (br *rows) Close() error {
	br.index = len(br.rows)
	br.haveRow = false
	return nil
}

func (br *rows) Next(ctx context.Context, dest []sql.Value) error {
	for br.index < len(br.rows) {
		if br.rows[br.index] != nil {
			copy(dest, br.rows[br.index])
			br.index += 1
			br.haveRow = true
			return nil
		}
		br.index += 1
	}

	br.haveRow = false
	return io.EOF
}

func (br *rows) Delete(ctx context.Context) error {
	if !br.haveRow {
		return fmt.Errorf("basic: no row to delete")
	}
	br.haveRow = false
	br.rows[br.index-1] = nil
	return nil
}

func (br *rows) Update(ctx context.Context, updates []db.ColumnUpdate) error {
	if !br.haveRow {
		return fmt.Errorf("basic: no row to update")
	}
	row := br.rows[br.index-1]
	for _, up := range updates {
		row[up.Index] = up.Value
	}
	return nil
}
