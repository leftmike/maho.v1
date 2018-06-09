package basic

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type basic struct{}

type database struct {
	name   sql.Identifier
	tables map[sql.Identifier]*table
}

type table struct {
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

func (be *basic) AttachDatabase(name sql.Identifier, path string,
	options engine.Options) (engine.Database, error) {

	return nil, fmt.Errorf("basic: attach database not supported")
}

func (be *basic) CreateDatabase(name sql.Identifier, path string,
	options engine.Options) (engine.Database, error) {

	return &database{
		name:   name,
		tables: map[sql.Identifier]*table{},
	}, nil
}

func (bdb *database) Message() string {
	return ""
}

func (bdb *database) LookupTable(ses db.Session, tctx interface{},
	tblname sql.Identifier) (db.Table, error) {

	tbl, ok := bdb.tables[tblname]
	if !ok {
		return nil, fmt.Errorf("basic: table %s not found in database %s", tblname, bdb.name)
	}
	return tbl, nil
}

func (bdb *database) CreateTable(ses db.Session, tctx interface{}, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	if _, dup := bdb.tables[tblname]; dup {
		return fmt.Errorf("basic: table %s already exists in database %s", tblname, bdb.name)
	}

	bdb.tables[tblname] = &table{
		columns:     cols,
		columnTypes: colTypes,
		rows:        nil,
	}
	return nil
}

func (bdb *database) DropTable(ses db.Session, tctx interface{}, tblname sql.Identifier,
	exists bool) error {

	if _, ok := bdb.tables[tblname]; !ok {
		if exists {
			return nil
		}
		return fmt.Errorf("basic: table %s does not exist in database %s", tblname, bdb.name)
	}
	delete(bdb.tables, tblname)
	return nil
}

func (bdb *database) ListTables(ses db.Session, tctx interface{}) ([]engine.TableEntry, error) {

	var tbls []engine.TableEntry
	for name, _ := range bdb.tables {
		tbls = append(tbls, engine.TableEntry{
			Name: name,
			Type: engine.PhysicalType,
		})
	}
	return tbls, nil
}

func (bdb *database) Begin(tx *engine.Transaction) interface{} {
	return nil
}

func (bdb *database) Commit(ses db.Session, tctx interface{}) error {
	return nil
}

func (bdb *database) Rollback(tctx interface{}) error {
	return nil
}

func (bdb *database) NextStmt(tctx interface{}) {}

func (bt *table) Columns(ses db.Session) []sql.Identifier {
	return bt.columns
}

func (bt *table) ColumnTypes(ses db.Session) []db.ColumnType {
	return bt.columnTypes
}

func (bt *table) Rows(ses db.Session) (db.Rows, error) {
	return &rows{columns: bt.columns, rows: bt.rows}, nil
}

func (bt *table) Insert(ses db.Session, row []sql.Value) error {
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

func (br *rows) Next(ses db.Session, dest []sql.Value) error {
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

func (br *rows) Delete(ses db.Session) error {
	if !br.haveRow {
		return fmt.Errorf("basic: no row to delete")
	}
	br.haveRow = false
	br.rows[br.index-1] = nil
	return nil
}

func (br *rows) Update(ses db.Session, updates []db.ColumnUpdate) error {
	if !br.haveRow {
		return fmt.Errorf("basic: no row to update")
	}
	row := br.rows[br.index-1]
	for _, up := range updates {
		row[up.Index] = up.Value
	}
	return nil
}
