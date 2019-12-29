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
	schemas map[sql.Identifier]*schema
}

type schema struct {
	tables map[sql.Identifier]*table
}

type index struct {
	keys   []engine.ColumnKey
	unique bool
}

type table struct {
	be          *basicEngine
	tn          sql.TableName
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	rows        [][]sql.Value
	indexes     map[sql.Identifier]*index
}

type rows struct {
	be      *basicEngine
	tn      sql.TableName
	columns []sql.Identifier
	rows    [][]sql.Value
	index   int
	haveRow bool
}

func NewEngine(dataDir string) (engine.Engine, error) {
	be := &basicEngine{
		databases: map[sql.Identifier]*database{},
	}
	ve := virtual.NewEngine(be)
	return ve, nil
}

func (_ *basicEngine) CreateSystemTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("basic: use virtual engine with basic engine")
}

func (_ *basicEngine) CreateInfoTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("basic: use virtual engine with basic engine")
}

func (be *basicEngine) CreateDatabase(dbname sql.Identifier, options engine.Options) error {
	be.mutex.Lock()
	defer be.mutex.Unlock()

	if _, ok := be.databases[dbname]; ok {
		return fmt.Errorf("basic: database %s already exists", dbname)
	}
	be.databases[dbname] = &database{
		be: be,
		schemas: map[sql.Identifier]*schema{
			sql.PUBLIC: &schema{
				tables: map[sql.Identifier]*table{},
			},
		},
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
	return bdb.createSchema(sn)
}

func (be *basicEngine) DropSchema(ctx context.Context, tx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[sn.Database]
	if !ok {
		return fmt.Errorf("basic: database %s not found", sn.Database)
	}
	return bdb.dropSchema(sn, ifExists)
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
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return fmt.Errorf("basic: database %s not found", tn.Database)
	}
	return bdb.createTable(ctx, tx, tn, cols, colTypes, primary, ifNotExists)
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

func (be *basicEngine) CreateIndex(ctx context.Context, tx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, unique bool, keys []engine.ColumnKey,
	ifNotExists bool) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return fmt.Errorf("basic: database %s not found", tn.Database)
	}
	return bdb.createIndex(ctx, tx, idxname, tn, unique, keys, ifNotExists)
}

func (be *basicEngine) DropIndex(ctx context.Context, tx engine.Transaction, idxname sql.Identifier,
	tn sql.TableName, ifExists bool) error {

	be.mutex.Lock()
	defer be.mutex.Unlock()

	bdb, ok := be.databases[tn.Database]
	if !ok {
		return fmt.Errorf("basic: database %s not found", tn.Database)
	}
	return bdb.dropIndex(ctx, tx, idxname, tn, ifExists)
}

func (_ *basicEngine) Begin(sesid uint64) engine.Transaction {
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

func (be *basicEngine) ListSchemas(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[dbname]
	if !ok {
		return nil, fmt.Errorf("basic: database %s not found", dbname)
	}

	var scnames []sql.Identifier
	for scname := range bdb.schemas {
		scnames = append(scnames, scname)
	}
	return scnames, nil
}

func (be *basicEngine) ListTables(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	be.mutex.RLock()
	defer be.mutex.RUnlock()

	bdb, ok := be.databases[sn.Database]
	if !ok {
		return nil, fmt.Errorf("basic: database %s not found", sn.Database)
	}

	bsc, ok := bdb.schemas[sn.Schema]
	if !ok {
		return nil, fmt.Errorf("basic: schema %s not found", sn)
	}

	var tblnames []sql.Identifier
	for tblname := range bsc.tables {
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

func (bdb *database) createSchema(sn sql.SchemaName) error {
	if _, ok := bdb.schemas[sn.Schema]; ok {
		return fmt.Errorf("basic: schema %s already exists", sn)
	}
	bdb.schemas[sn.Schema] = &schema{
		tables: map[sql.Identifier]*table{},
	}
	return nil
}

func (bdb *database) dropSchema(sn sql.SchemaName, ifExists bool) error {
	bsc, ok := bdb.schemas[sn.Schema]
	if !ok {
		if ifExists {
			return nil
		}
		return fmt.Errorf("basic: schema %s not found", sn)
	}
	if len(bsc.tables) > 0 {
		return fmt.Errorf("basic: schema %s is not empty", sn)
	}

	delete(bdb.schemas, sn.Schema)
	return nil
}

func (bdb *database) lookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	bsc, ok := bdb.schemas[tn.Schema]
	if !ok {
		return nil, fmt.Errorf("basic: schema %s not found", tn.SchemaName())
	}

	tbl, ok := bsc.tables[tn.Table]
	if !ok {
		return nil, fmt.Errorf("basic: table %s not found", tn)
	}
	return tbl, nil
}

func (bdb *database) createTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	bsc, ok := bdb.schemas[tn.Schema]
	if !ok {
		return fmt.Errorf("basic: schema %s not found", tn.SchemaName())
	}

	if _, dup := bsc.tables[tn.Table]; dup {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("basic: table %s already exists", tn)
	}

	tbl := &table{
		be:          bdb.be,
		tn:          tn,
		columns:     cols,
		columnTypes: colTypes,
		rows:        nil,
		indexes:     map[sql.Identifier]*index{},
	}
	if primary != nil {
		tbl.indexes[sql.PRIMARY] = &index{
			keys:   primary,
			unique: true,
		}
	}
	bsc.tables[tn.Table] = tbl
	return nil
}

func (bdb *database) dropTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	bsc, ok := bdb.schemas[tn.Schema]
	if !ok {
		return fmt.Errorf("basic: schema %s not found", tn.SchemaName())
	}

	if _, ok := bsc.tables[tn.Table]; !ok {
		if ifExists {
			return nil
		}
		return fmt.Errorf("basic: table %s does not exist", tn)
	}
	delete(bsc.tables, tn.Table)
	return nil
}

func (bdb *database) createIndex(ctx context.Context, tx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, unique bool, keys []engine.ColumnKey,
	ifNotExists bool) error {

	bsc, ok := bdb.schemas[tn.Schema]
	if !ok {
		return fmt.Errorf("basic: schema %s not found", tn.SchemaName())
	}

	tbl, ok := bsc.tables[tn.Table]
	if !ok {
		return fmt.Errorf("basic: table %s not found", tn)
	}

	if _, dup := tbl.indexes[idxname]; dup {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("basic: index %s already exists in table %s", idxname, tn)
	}

	tbl.indexes[idxname] = &index{
		keys:   keys,
		unique: unique,
	}
	return nil
}

func (bdb *database) dropIndex(ctx context.Context, tx engine.Transaction, idxname sql.Identifier,
	tn sql.TableName, ifExists bool) error {

	bsc, ok := bdb.schemas[tn.Schema]
	if !ok {
		return fmt.Errorf("basic: schema %s not found", tn.SchemaName())
	}

	tbl, ok := bsc.tables[tn.Table]
	if !ok {
		return fmt.Errorf("basic: table %s not found", tn)
	}

	if _, ok := tbl.indexes[idxname]; !ok {
		if ifExists {
			return nil
		}
		return fmt.Errorf("basic: index %s does not exist in table %s", idxname, tn)
	}
	delete(tbl.indexes, idxname)
	return nil
}

func (bt *table) Columns(ctx context.Context) []sql.Identifier {
	return bt.columns
}

func (bt *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return bt.columnTypes
}

func (bt *table) PrimaryKey(ctx context.Context) []engine.ColumnKey {
	return nil
}

func (bt *table) Seek(ctx context.Context, row []sql.Value) (engine.Rows, error) {
	return bt.Rows(ctx)
}

func (bt *table) Rows(ctx context.Context) (engine.Rows, error) {
	bt.be.mutex.RLock()
	defer bt.be.mutex.RUnlock()

	return &rows{be: bt.be, tn: bt.tn, columns: bt.columns, rows: bt.rows}, nil
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
		return fmt.Errorf("basic: table %s no row to delete", br.tn)
	}
	br.haveRow = false
	br.rows[br.index-1] = nil
	return nil
}

func (br *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	br.be.mutex.Lock()
	defer br.be.mutex.Unlock()

	if !br.haveRow {
		return fmt.Errorf("basic: table %s no row to update", br.tn)
	}
	row := br.rows[br.index-1]
	for _, up := range updates {
		row[up.Index] = up.Value
	}
	return nil
}
