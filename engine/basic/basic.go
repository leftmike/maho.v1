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
	be      *basicEngine
	name    sql.Identifier
	schemas map[sql.Identifier]struct{}
	tables  map[sql.Identifier]*table
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

func (be *basicEngine) CreateDatabase(dbname sql.Identifier, options engine.Options) error {
	be.mutex.Lock()
	defer be.mutex.Unlock()

	if _, ok := be.databases[dbname]; ok {
		return fmt.Errorf("basic: database %s already exists", dbname)
	}
	be.databases[dbname] = &database{
		be:   be,
		name: dbname,
		schemas: map[sql.Identifier]struct{}{
			sql.PUBLIC: struct{}{},
		},
		tables: map[sql.Identifier]*table{},
	}
	return nil
}

func (be *basicEngine) DropDatabase(dbname sql.Identifier, ifExists bool,
	options engine.Options) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	_, ok := be.databases[dbname]
	if !ok {
		if ifExists {
			return nil
		}
		return fmt.Errorf("basic: database %s does not exist", dbname)
	}
	delete(be.databases, dbname)
	return nil
}

func (be *basicEngine) CreateSchema(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[sn.Database]
	if !ok {
		return fmt.Errorf("basic: database %s not found", sn.Database)
	}
	return bdb.createSchema(sn.Schema)
}

func (be *basicEngine) DropSchema(ctx context.Context, tx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[sn.Database]
	if !ok {
		return fmt.Errorf("basic: database %s not found", sn.Database)
	}
	return bdb.dropSchema(sn.Schema, ifExists)
}

func (be *basicEngine) LookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return nil, fmt.Errorf("basic: database %s not found", tn.Database)
	}
	return bdb.lookupTable(ctx, tx, tn)
}

func (be *basicEngine) CreateTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return fmt.Errorf("basic: database %s not found", tn.Database)
	}
	return bdb.createTable(ctx, tx, tn, cols, colTypes)
}

func (be *basicEngine) DropTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return fmt.Errorf("basic: database %s not found", tn.Database)
	}
	return bdb.dropTable(ctx, tx, tn, ifExists)
}

func (_ *basicEngine) Begin(sid uint64) engine.Transaction {
	return &transaction{}
}

func (_ *basicEngine) IsTransactional() bool {
	return false
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
	dbname sql.Identifier) ([]sql.Identifier, error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[dbname]
	if !ok {
		return nil, fmt.Errorf("basic: database %s not found", dbname)
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

func (bdb *database) createSchema(scname sql.Identifier) error {
	if _, ok := bdb.schemas[scname]; ok {
		return fmt.Errorf("basic: schema %s.%s already exists", bdb.name, scname)
	}
	bdb.schemas[scname] = struct{}{}
	return nil
}

func (bdb *database) dropSchema(scname sql.Identifier, ifExists bool) error {
	if _, ok := bdb.schemas[scname]; !ok {
		if ifExists {
			return nil
		}
		return fmt.Errorf("basic: schema %s.%s already exists", bdb.name, scname)
	}

	// XXX: check to make sure no tables in the schema
	delete(bdb.schemas, scname)
	return nil
}

func (bdb *database) lookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	tbl, ok := bdb.tables[tn.Table]
	if !ok {
		return nil, fmt.Errorf("basic: table %s not found", tn)
	}
	return tbl, nil
}

func (bdb *database) createTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType) error {

	if _, dup := bdb.tables[tn.Table]; dup {
		return fmt.Errorf("basic: table %s already exists", tn)
	}

	bdb.tables[tn.Table] = &table{
		be:          bdb.be,
		name:        tn.String(),
		columns:     cols,
		columnTypes: colTypes,
		rows:        nil,
	}
	return nil
}

func (bdb *database) dropTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	if _, ok := bdb.tables[tn.Table]; !ok {
		if ifExists {
			return nil
		}
		return fmt.Errorf("basic: table %s does not exist", tn)
	}
	delete(bdb.tables, tn.Table)
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
