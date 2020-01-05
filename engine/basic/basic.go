package basic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/btree"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/util"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
)

const (
	schemasMID = 1
	tablesMID  = 2
	indexesMID = 3
)

var (
	errTransactionComplete = errors.New("basic: transaction already completed")

	schemasTableDef = &tableDef{
		tn:          sql.TableName{sql.ID("system"), sql.ID("private"), sql.ID("schemas")},
		columns:     []sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("tables")},
		columnTypes: []sql.ColumnType{sql.IdColType, sql.IdColType, sql.Int64ColType},
		primary: []engine.ColumnKey{
			engine.MakeColumnKey(0, false),
			engine.MakeColumnKey(1, false),
		},
		mid: schemasMID,
	}

	tablesTableDef = &tableDef{
		tn: sql.TableName{sql.ID("system"), sql.ID("private"), sql.ID("tables")},
		columns: []sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("table"),
			sql.ID("mid")},
		columnTypes: []sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType,
			sql.Int64ColType},
		primary: []engine.ColumnKey{
			engine.MakeColumnKey(0, false),
			engine.MakeColumnKey(1, false),
			engine.MakeColumnKey(2, false),
		},
		mid: tablesMID,
	}

	indexesTableDef = &tableDef{
		tn: sql.TableName{sql.ID("system"), sql.ID("private"), sql.ID("indexes")},
		columns: []sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("table"),
			sql.ID("index")},
		columnTypes: []sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType, sql.IdColType},
		primary: []engine.ColumnKey{
			engine.MakeColumnKey(0, false),
			engine.MakeColumnKey(1, false),
			engine.MakeColumnKey(2, false),
			engine.MakeColumnKey(3, false),
		},
		mid: indexesMID,
	}
)

type basicEngine struct {
	mutex       sync.Mutex
	databases   map[sql.Identifier]struct{}
	definitions map[uint64]*tableDef
	tree        *btree.BTree
	lastMID     uint64
	lastRID     uint64
}

type transaction struct {
	be   *basicEngine
	tree *btree.BTree
}

type tableDef struct {
	tn          sql.TableName
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	primary     []engine.ColumnKey
	mid         uint64
}

type table struct {
	be  *basicEngine
	tx  *transaction
	def *tableDef
}

type midRow struct {
	def *tableDef
	rid uint64
	row []sql.Value
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
	rids []uint64
}

func NewEngine(dataDir string) (engine.Engine, error) {
	be := &basicEngine{
		databases:   map[sql.Identifier]struct{}{},
		definitions: map[uint64]*tableDef{},
		tree:        btree.New(16),
		lastMID:     63,
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

func (be *basicEngine) createDatabase(dbname sql.Identifier) error {
	be.mutex.Lock()
	defer be.mutex.Unlock()

	if _, ok := be.databases[dbname]; ok {
		return fmt.Errorf("basic: database %s already exists", dbname)
	}
	be.databases[dbname] = struct{}{}

	return nil
}

func (be *basicEngine) setupDatabase(dbname sql.Identifier) error {
	ctx := context.Background()
	etx := be.Begin(0)
	err := be.CreateSchema(ctx, etx, sql.SchemaName{dbname, sql.PUBLIC})
	if err != nil {
		etx.Rollback()
		return err
	}
	return etx.Commit(ctx)
}

func (be *basicEngine) CreateDatabase(dbname sql.Identifier, options engine.Options) error {
	if len(options) != 0 {
		return fmt.Errorf("basic: unexpected option to create database: %s", dbname)
	}

	err := be.createDatabase(dbname)
	if err != nil {
		return err
	}
	err = be.setupDatabase(dbname)
	if err != nil {
		be.mutex.Lock()
		delete(be.databases, dbname)
		be.mutex.Unlock()
		return err
	}
	return nil
}

func (be *basicEngine) cleanupDatabase(dbname sql.Identifier) error {
	ctx := context.Background()
	etx := be.Begin(0)
	scnames, err := be.ListSchemas(ctx, etx, dbname)
	if err != nil {
		etx.Rollback()
		return err
	}
	for _, scname := range scnames {
		err = be.DropSchema(ctx, etx, sql.SchemaName{dbname, scname}, true)
		if err != nil {
			etx.Rollback()
			return err
		}
	}
	return etx.Commit(ctx)
}

func (be *basicEngine) DropDatabase(dbname sql.Identifier, ifExists bool,
	options engine.Options) error {

	if len(options) != 0 {
		return fmt.Errorf("basic: unexpected option to drop database: %s", dbname)
	}

	err := be.cleanupDatabase(dbname)
	if err != nil {
		return err
	}

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

func (be *basicEngine) Name() string {
	return "basic"
}

func (be *basicEngine) AllocateMID(ctx context.Context) (uint64, error) {
	be.lastMID += 1
	return be.lastMID, nil
}

func (be *basicEngine) MakeSchemasTable(etx engine.Transaction) *util.TypedTable {
	return util.MakeTypedTable(schemasTableDef.tn,
		&table{
			be:  be,
			tx:  etx.(*transaction),
			def: schemasTableDef,
		})
}

func (be *basicEngine) CreateSchema(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) error {

	_, ok := be.databases[sn.Database]
	if !ok {
		return fmt.Errorf("basic: database %s not found", sn.Database)
	}

	return util.CreateSchema(ctx, be, etx, sn)
}

func (be *basicEngine) DropSchema(ctx context.Context, etx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	return util.DropSchema(ctx, be, etx, sn, ifExists)
}

func (be *basicEngine) MakeTablesTable(etx engine.Transaction) *util.TypedTable {
	return util.MakeTypedTable(tablesTableDef.tn,
		&table{
			be:  be,
			tx:  etx.(*transaction),
			def: tablesTableDef,
		})
}

func (be *basicEngine) LookupTable(ctx context.Context, etx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	mid, err := util.LookupTable(ctx, be, etx, tn)
	if err != nil {
		return nil, err
	} else if mid == 0 {
		return nil, fmt.Errorf("basic: table %s not found", tn)
	}

	def, ok := be.definitions[mid]
	if !ok {
		panic(fmt.Sprintf("basic: table %s missing table definition", tn))
	}

	return &table{
		be:  be,
		tx:  etx.(*transaction),
		def: def,
	}, nil
}

func (be *basicEngine) CreateTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	mid, err := util.CreateTable(ctx, be, etx, tn, ifNotExists)
	if err != nil {
		return err
	}
	if mid == 0 {
		return nil
	}

	be.definitions[mid] =
		&tableDef{
			tn:          tn,
			columns:     cols,
			columnTypes: colTypes,
			primary:     primary,
			mid:         mid,
		}
	return nil
}

func (be *basicEngine) DropTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	return util.DropTable(ctx, be, etx, tn, ifExists)
}

func (be *basicEngine) MakeIndexesTable(etx engine.Transaction) *util.TypedTable {
	return util.MakeTypedTable(indexesTableDef.tn,
		&table{
			be:  be,
			tx:  etx.(*transaction),
			def: indexesTableDef,
		})
}

func (be *basicEngine) CreateIndex(ctx context.Context, etx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, unique bool, keys []engine.ColumnKey,
	ifNotExists bool) error {

	return util.CreateIndex(ctx, be, etx, idxname, tn, ifNotExists)
}

func (be *basicEngine) DropIndex(ctx context.Context, etx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, ifExists bool) error {

	return util.DropIndex(ctx, be, etx, idxname, tn, ifExists)
}

func (be *basicEngine) Begin(sesid uint64) engine.Transaction {
	be.mutex.Lock()
	return &transaction{
		be:   be,
		tree: be.tree,
	}
}

func (be *basicEngine) ListDatabases(ctx context.Context, tx engine.Transaction) ([]sql.Identifier,
	error) {

	var dbnames []sql.Identifier
	for dbname := range be.databases {
		dbnames = append(dbnames, dbname)
	}
	return dbnames, nil
}

func (be *basicEngine) ListSchemas(ctx context.Context, etx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	return util.ListSchemas(ctx, be, etx, dbname)
}

func (be *basicEngine) ListTables(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	return util.ListTables(ctx, be, etx, sn)
}

func (btx *transaction) Commit(ctx context.Context) error {
	if btx.be == nil {
		return errTransactionComplete
	}

	btx.be.tree = btx.tree
	btx.be.mutex.Unlock()
	btx.be = nil
	btx.tree = nil
	return nil
}

func (btx *transaction) Rollback() error {
	if btx.be == nil {
		return errTransactionComplete
	}

	btx.be.mutex.Unlock()
	btx.be = nil
	btx.tree = nil
	return nil
}

func (_ *transaction) NextStmt() {}

func (btx *transaction) forWrite() {
	if btx.tree == btx.be.tree {
		btx.tree = btx.be.tree.Clone()
	}
}

func (mr midRow) Less(item btree.Item) bool {
	mr2 := item.(midRow)
	if mr.def.mid < mr2.def.mid {
		return true
	} else if mr.def != mr2.def {
		return false
	} else if mr.rid < mr2.rid {
		return true
	} else if mr.rid > mr2.rid {
		return false
	} else if mr2.row == nil {
		return false
	} else if mr.row == nil {
		return true
	}

	for _, ck := range mr.def.primary {
		idx := ck.Number()
		cmp := sql.Compare(mr.row[idx], mr2.row[idx])
		if cmp == 0 {
			continue
		}
		if ck.Reverse() {
			return cmp > 0
		} else {
			return cmp < 0
		}
	}

	return false
}

func (bt *table) Columns(ctx context.Context) []sql.Identifier {
	return bt.def.columns
}

func (bt *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return bt.def.columnTypes
}

func (bt *table) PrimaryKey(ctx context.Context) []engine.ColumnKey {
	return bt.def.primary
}

func (bt *table) Seek(ctx context.Context, row []sql.Value) (engine.Rows, error) {
	br := &rows{
		tbl: bt,
		idx: 0,
	}
	bt.tx.tree.AscendGreaterOrEqual(midRow{def: bt.def, row: row}, br.itemIterator)
	return br, nil
}

func (bt *table) Rows(ctx context.Context) (engine.Rows, error) {
	return bt.Seek(ctx, nil)
}

func (bt *table) Insert(ctx context.Context, row []sql.Value) error {
	bt.tx.forWrite()

	var rid uint64
	if bt.def.primary == nil {
		bt.be.lastRID += 1
		rid = bt.be.lastRID
	} else {
		if bt.tx.tree.Has(midRow{def: bt.def, row: row}) {
			return fmt.Errorf("basic: %s: existing row with duplicate primary key", bt.def.tn)
		}
	}

	bt.tx.tree.ReplaceOrInsert(
		midRow{def: bt.def, rid: rid, row: append(make([]sql.Value, 0, len(row)), row...)})
	return nil
}

func (br *rows) itemIterator(item btree.Item) bool {
	mr := item.(midRow)
	if mr.def.mid != br.tbl.def.mid {
		return false
	}
	br.rows = append(br.rows, mr.row)
	if br.tbl.def.primary == nil {
		br.rids = append(br.rids, mr.rid)
	}
	return true
}

func (br *rows) Columns() []sql.Identifier {
	return br.tbl.def.columns
}

func (br *rows) Close() error {
	br.tbl = nil
	br.rows = nil
	br.rids = nil
	br.idx = 0
	return nil
}

func (br *rows) Next(ctx context.Context, dest []sql.Value) error {
	if br.idx == len(br.rows) {
		return io.EOF
	}

	copy(dest, br.rows[br.idx])
	br.idx += 1
	return nil
}

func (br *rows) Delete(ctx context.Context) error {
	br.tbl.tx.forWrite()

	if br.idx == 0 {
		panic(fmt.Sprintf("basic: table %s no row to delete", br.tbl.def.tn))
	}

	var rid uint64
	if br.tbl.def.primary == nil {
		rid = br.rids[br.idx-1]
	}
	br.tbl.tx.tree.Delete(midRow{def: br.tbl.def, rid: rid, row: br.rows[br.idx-1]})
	return nil
}

func (br *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	br.tbl.tx.forWrite()

	if br.idx == 0 {
		panic(fmt.Sprintf("basic: table %s no row to delete", br.tbl.def.tn))
	}

	var primaryUpdated bool
	for _, update := range updates {
		for _, ck := range br.tbl.def.primary {
			if ck.Number() == update.Index {
				primaryUpdated = true
			}
		}
	}

	if primaryUpdated {
		br.Delete(ctx)

		for _, update := range updates {
			br.rows[br.idx-1][update.Index] = update.Value
		}

		return br.tbl.Insert(ctx, br.rows[br.idx-1])
	}

	var rid uint64
	if br.tbl.def.primary == nil {
		rid = br.rids[br.idx-1]
	}

	row := append(make([]sql.Value, 0, len(br.rows[br.idx-1])), br.rows[br.idx-1]...)
	for _, update := range updates {
		row[update.Index] = update.Value
	}
	br.tbl.tx.tree.ReplaceOrInsert(midRow{def: br.tbl.def, rid: rid, row: row})
	return nil
}
