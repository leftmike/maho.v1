package basic

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
)

type basicEngine struct {
	mutex     sync.RWMutex
	databases map[sql.Identifier]*database
}

type transaction struct{}

type database struct {
	be     *basicEngine
	name   sql.Identifier
	tables map[sql.Identifier]*table
}

type table struct {
	be          *basicEngine
	name        string
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	rows        [][]sql.Value
}

type rows struct {
	be      *basicEngine
	name    string
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
	haveRow bool
}

func NewEngine(dataDir string) engine.Engine {
	be := &basicEngine{
		databases: map[sql.Identifier]*database{},
	}
	ve := virtual.NewEngine(be)
	return ve
}

func (_ *basicEngine) CreateSystemTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("memrows: use virtual engine with memrows engine")
}

func (_ *basicEngine) CreateInfoTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("memrows: use virtual engine with memrows engine")
}

func (_ *basicEngine) AttachDatabase(name sql.Identifier, options engine.Options) error {
	return fmt.Errorf("basic: attach database not supported")
}

func (be *basicEngine) CreateDatabase(name sql.Identifier, options engine.Options) error {
	be.mutex.Lock()
	defer be.mutex.Unlock()

	if _, ok := be.databases[name]; ok {
		return fmt.Errorf("basic: database %s already exists", name)
	}
	be.databases[name] = &database{
		be:     be,
		name:   name,
		tables: map[sql.Identifier]*table{},
	}
	return nil
}

func (be *basicEngine) DetachDatabase(name sql.Identifier, options engine.Options) error {
	return fmt.Errorf("basic: detach database not supported")
}

func (be *basicEngine) DropDatabase(name sql.Identifier, exists bool,
	options engine.Options) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	_, ok := be.databases[name]
	if !ok {
		if exists {
			return nil
		}
		return fmt.Errorf("basic: database %s does not exist", name)
	}
	delete(be.databases, name)
	return nil
}

func (be *basicEngine) LookupTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier) (engine.Table, error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[dbname]
	if !ok {
		return nil, fmt.Errorf("basic: database %s not found", dbname)
	}
	return bdb.lookupTable(ctx, tx, tblname)
}

func (be *basicEngine) CreateTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier, cols []sql.Identifier, colTypes []sql.ColumnType) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[dbname]
	if !ok {
		return fmt.Errorf("basic: database %s not found", dbname)
	}
	return bdb.createTable(ctx, tx, tblname, cols, colTypes)
}

func (be *basicEngine) DropTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier, exists bool) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[dbname]
	if !ok {
		return fmt.Errorf("basic: database %s not found", dbname)
	}
	return bdb.dropTable(ctx, tx, tblname, exists)
}

func (_ *basicEngine) Begin(sid uint64) engine.Transaction {
	return &transaction{}
}

func (be *basicEngine) ListDatabases(ctx context.Context, tx engine.Transaction) ([]sql.Identifier,
	error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	var dbnames []sql.Identifier
	for dbname := range be.databases {
		dbnames = append(dbnames, dbname)
	}
	return dbnames, nil
}

func (be *basicEngine) ListTables(ctx context.Context, tx engine.Transaction,
	name sql.Identifier) ([]sql.Identifier, error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[name]
	if !ok {
		return nil, fmt.Errorf("basic: database %s not found", name)
	}

	var tblnames []sql.Identifier
	for tblname := range bdb.tables {
		tblnames = append(tblnames, tblname)
	}
	return tblnames, nil
}

func (_ *transaction) Commit(ctx context.Context) error {
	return nil
}

func (_ *transaction) Rollback() error {
	return nil
}

func (_ *transaction) NextStmt() {}

func (bdb *database) Message() string {
	return ""
}

func (bdb *database) lookupTable(ctx context.Context, tx engine.Transaction,
	tblname sql.Identifier) (engine.Table, error) {

	tbl, ok := bdb.tables[tblname]
	if !ok {
		return nil, fmt.Errorf("basic: table %s.%s not found", bdb.name, tblname)
	}
	return tbl, nil
}

func (bdb *database) createTable(ctx context.Context, tx engine.Transaction, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []sql.ColumnType) error {

	if _, dup := bdb.tables[tblname]; dup {
		return fmt.Errorf("basic: table %s.%s already exists", bdb.name, tblname)
	}

	bdb.tables[tblname] = &table{
		be:          bdb.be,
		name:        fmt.Sprintf("%s.%s", bdb.name, tblname),
		columns:     cols,
		columnTypes: colTypes,
		rows:        nil,
	}
	return nil
}

func (bdb *database) dropTable(ctx context.Context, tx engine.Transaction, tblname sql.Identifier,
	exists bool) error {

	if _, ok := bdb.tables[tblname]; !ok {
		if exists {
			return nil
		}
		return fmt.Errorf("basic: table %s.%s does not exist", bdb.name, tblname)
	}
	delete(bdb.tables, tblname)
	return nil
}

func (bt *table) Columns(ctx context.Context) []sql.Identifier {
	return bt.columns
}

func (bt *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return bt.columnTypes
}

func (bt *table) Rows(ctx context.Context) (engine.Rows, error) {
	bt.be.mutex.RLock()
	defer bt.be.mutex.RUnlock()

	return &rows{be: bt.be, name: bt.name, columns: bt.columns, rows: bt.rows}, nil
}

func (bt *table) Insert(ctx context.Context, row []sql.Value) error {
	bt.be.mutex.Lock()
	defer bt.be.mutex.Unlock()

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
	br.be.mutex.RLock()
	defer br.be.mutex.RUnlock()

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
	br.be.mutex.Lock()
	defer br.be.mutex.Unlock()

	if !br.haveRow {
		return fmt.Errorf("basic: table %s no row to delete", br.name)
	}
	br.haveRow = false
	br.rows[br.index-1] = nil
	return nil
}

func (br *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	br.be.mutex.Lock()
	defer br.be.mutex.Unlock()

	if !br.haveRow {
		return fmt.Errorf("basic: table %s no row to update", br.name)
	}
	row := br.rows[br.index-1]
	for _, up := range updates {
		row[up.Index] = up.Value
	}
	return nil
}
