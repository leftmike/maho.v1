package basic

import (
	"fmt"
	"io"
	"sync"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/fatlock"
	"github.com/leftmike/maho/sql"
)

var mutex sync.RWMutex

type Engine struct{}

type database struct {
	name   sql.Identifier
	tables map[sql.Identifier]*table
}

type table struct {
	name        string
	columns     []sql.Identifier
	columnTypes []db.ColumnType
	rows        [][]sql.Value
}

type rows struct {
	name    string
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
	haveRow bool
}

func (_ Engine) AttachDatabase(name sql.Identifier, path string,
	options engine.Options) (engine.Database, error) {

	return nil, fmt.Errorf("basic: attach database not supported")
}

func (_ Engine) CreateDatabase(name sql.Identifier, path string,
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

	mutex.RLock()
	defer mutex.RUnlock()

	tbl, ok := bdb.tables[tblname]
	if !ok {
		return nil, fmt.Errorf("basic: table %s.%s not found", bdb.name, tblname)
	}
	return tbl, nil
}

func (bdb *database) CreateTable(ses db.Session, tctx interface{}, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	mutex.Lock()
	defer mutex.Unlock()

	if _, dup := bdb.tables[tblname]; dup {
		return fmt.Errorf("basic: table %s.%s already exists", bdb.name, tblname)
	}

	bdb.tables[tblname] = &table{
		name:        fmt.Sprintf("%s.%s", bdb.name, tblname),
		columns:     cols,
		columnTypes: colTypes,
		rows:        nil,
	}
	return nil
}

func (bdb *database) DropTable(ses db.Session, tctx interface{}, tblname sql.Identifier,
	exists bool) error {

	mutex.Lock()
	defer mutex.Unlock()

	if _, ok := bdb.tables[tblname]; !ok {
		if exists {
			return nil
		}
		return fmt.Errorf("basic: table %s.%s does not exist", bdb.name, tblname)
	}
	delete(bdb.tables, tblname)
	return nil
}

func (bdb *database) ListTables(ses db.Session, tctx interface{}) ([]engine.TableEntry, error) {
	mutex.RLock()
	defer mutex.RUnlock()

	var tbls []engine.TableEntry
	for name, _ := range bdb.tables {
		tbls = append(tbls, engine.TableEntry{
			Name: name,
			Type: engine.PhysicalType,
		})
	}
	return tbls, nil
}

func (bdb *database) Begin(lkr fatlock.Locker) interface{} {
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
	mutex.RLock()
	defer mutex.RUnlock()

	return &rows{name: bt.name, columns: bt.columns, rows: bt.rows}, nil
}

func (bt *table) Insert(ses db.Session, row []sql.Value) error {
	mutex.Lock()
	defer mutex.Unlock()

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
	mutex.RLock()
	defer mutex.RUnlock()

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
	mutex.Lock()
	defer mutex.Unlock()

	if !br.haveRow {
		return fmt.Errorf("basic: table %s no row to delete", br.name)
	}
	br.haveRow = false
	br.rows[br.index-1] = nil
	return nil
}

func (br *rows) Update(ses db.Session, updates []db.ColumnUpdate) error {
	mutex.Lock()
	defer mutex.Unlock()

	if !br.haveRow {
		return fmt.Errorf("basic: table %s no row to update", br.name)
	}
	row := br.rows[br.index-1]
	for _, up := range updates {
		row[up.Index] = up.Value
	}
	return nil
}
