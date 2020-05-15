package rowcols

//go:generate protoc --go_opt=paths=source_relative --go_out=. metadata.proto

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/btree"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/util"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

const (
	schemasMID = 1
	tablesMID  = 2
	indexesMID = 3
)

var (
	errTransactionComplete = errors.New("rowcols: transaction already completed")

	schemasTableDef = makeTableDef(
		sql.TableName{sql.ID("system"), sql.ID("private"), sql.ID("schemas")}, util.SchemasColumns,
		util.SchemasColumnTypes, util.SchemasPrimaryKey, schemasMID)

	tablesTableDef = makeTableDef(
		sql.TableName{sql.ID("system"), sql.ID("private"), sql.ID("tables")}, util.TablesColumns,
		util.TablesColumnTypes, util.TablesPrimaryKey, tablesMID)

	indexesTableDef = makeTableDef(
		sql.TableName{sql.ID("system"), sql.ID("private"), sql.ID("indexes")}, util.IndexesColumns,
		util.IndexesColumnTypes, util.IndexesPrimaryKey, indexesMID)
)

type rowColsEngine struct {
	dataDir     string
	mutex       sync.Mutex
	wal         *WAL
	databases   map[sql.Identifier]struct{}
	tableDefs   map[uint64]*tableDef
	tree        *btree.BTree
	ver         uint64
	lastMID     uint64
	commitMutex sync.Mutex
}

type transaction struct {
	rce   *rowColsEngine
	tree  *btree.BTree
	ver   uint64
	delta *btree.BTree
}

type tableDef struct {
	tn          sql.TableName
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	primary     []engine.ColumnKey
	mid         uint64

	reverse uint32
	rowCols []int
}

type table struct {
	rce *rowColsEngine
	tx  *transaction
	def *tableDef
}

type rowItem struct {
	mid        uint64
	ver        uint64
	reverse    uint32
	numKeyCols uint8
	deleted    bool
	row        []sql.Value
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
}

func NewEngine(dataDir string) (engine.Engine, error) {
	os.MkdirAll(dataDir, 0755)
	f, err := os.OpenFile(filepath.Join(dataDir, "mahorowcols.wal"), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	rce := &rowColsEngine{
		dataDir:   dataDir,
		wal:       &WAL{f: f},
		tableDefs: map[uint64]*tableDef{},
		tree:      btree.New(16),
		lastMID:   63,
	}
	err = rce.readDatabases()
	if err != nil {
		return nil, err
	}
	err = rce.wal.ReadWAL(rce)
	if err != nil {
		return nil, err
	}

	ve := virtual.NewEngine(rce)
	return ve, nil
}

func (_ *rowColsEngine) CreateSystemTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("rowcols: use virtual engine with rowcols engine")
}

func (_ *rowColsEngine) CreateInfoTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("rowcols: use virtual engine with rowcols engine")
}

func (rce *rowColsEngine) readDatabases() error {
	if rce.databases != nil {
		panic("rowColsEngine.readDatabases() should only be called once")
	}

	rce.databases = map[sql.Identifier]struct{}{}

	buf, err := ioutil.ReadFile(filepath.Join(rce.dataDir, "mahorowcols.databases"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, line := range strings.Split(string(buf), "\n") {
		dbname := strings.TrimSpace(line)
		if dbname == "" {
			continue
		}
		rce.databases[sql.ID(dbname)] = struct{}{}
	}
	return nil
}

func (rce *rowColsEngine) writeDatabases() error {
	f, err := os.Create(filepath.Join(rce.dataDir, "mahorowcols.databases"))
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for dbname := range rce.databases {
		fmt.Fprintln(w, dbname.String())
	}
	return w.Flush()
}

func (rce *rowColsEngine) createDatabase(dbname sql.Identifier) error {
	rce.mutex.Lock()
	defer rce.mutex.Unlock()

	if _, ok := rce.databases[dbname]; ok {
		return fmt.Errorf("rowcols: database %s already exists", dbname)
	}
	rce.databases[dbname] = struct{}{}

	return nil
}

func (rce *rowColsEngine) setupDatabase(dbname sql.Identifier) error {
	ctx := context.Background()
	etx := rce.Begin(0)
	err := rce.CreateSchema(ctx, etx, sql.SchemaName{dbname, sql.PUBLIC})
	if err != nil {
		etx.Rollback()
		return err
	}
	return etx.Commit(ctx)
}

func (rce *rowColsEngine) CreateDatabase(dbname sql.Identifier, options engine.Options) error {
	if len(options) != 0 {
		return fmt.Errorf("rowcols: unexpected option to create database: %s", dbname)
	}

	err := rce.createDatabase(dbname)
	if err != nil {
		return err
	}

	err = rce.setupDatabase(dbname)
	if err == nil {
		err = rce.writeDatabases()
	}
	if err != nil {
		rce.mutex.Lock()
		delete(rce.databases, dbname)
		rce.mutex.Unlock()
	}

	return err
}

func (rce *rowColsEngine) cleanupDatabase(dbname sql.Identifier) error {
	ctx := context.Background()
	etx := rce.Begin(0)
	scnames, err := rce.ListSchemas(ctx, etx, dbname)
	if err != nil {
		etx.Rollback()
		return err
	}
	for _, scname := range scnames {
		err = rce.DropSchema(ctx, etx, sql.SchemaName{dbname, scname}, true)
		if err != nil {
			etx.Rollback()
			return err
		}
	}
	return etx.Commit(ctx)
}

func (rce *rowColsEngine) dropDatabase(dbname sql.Identifier, ifExists bool) error {
	rce.mutex.Lock()
	defer rce.mutex.Unlock()

	_, ok := rce.databases[dbname]
	if !ok {
		if ifExists {
			return nil
		}
		return fmt.Errorf("rowcols: database %s does not exist", dbname)
	}
	delete(rce.databases, dbname)
	return nil
}

func (rce *rowColsEngine) DropDatabase(dbname sql.Identifier, ifExists bool,
	options engine.Options) error {

	if len(options) != 0 {
		return fmt.Errorf("rowcols: unexpected option to drop database: %s", dbname)
	}

	err := rce.cleanupDatabase(dbname)
	if err != nil {
		return err
	}

	err = rce.dropDatabase(dbname, ifExists)
	if err != nil {
		return err
	}

	err = rce.writeDatabases()
	if err != nil {
		rce.mutex.Lock()
		rce.databases[dbname] = struct{}{}
		rce.mutex.Unlock()
	}

	return err
}

func (rce *rowColsEngine) Name() string {
	return "rowcols"
}

func (rce *rowColsEngine) AllocateMID(ctx context.Context) (uint64, error) {
	rce.lastMID += 1
	return rce.lastMID, nil
}

func (rce *rowColsEngine) MakeSchemasTable(etx engine.Transaction) *util.TypedTable {
	return util.MakeTypedTable(schemasTableDef.tn,
		&table{
			rce: rce,
			tx:  etx.(*transaction),
			def: schemasTableDef,
		})
}

func (rce *rowColsEngine) CreateSchema(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) error {

	_, ok := rce.databases[sn.Database]
	if !ok {
		return fmt.Errorf("rowcols: database %s not found", sn.Database)
	}

	return util.CreateSchema(ctx, rce, etx, sn)
}

func (rce *rowColsEngine) DropSchema(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName, ifExists bool) error {

	return util.DropSchema(ctx, rce, etx, sn, ifExists)
}

func (rce *rowColsEngine) MakeTablesTable(etx engine.Transaction) *util.TypedTable {
	return util.MakeTypedTable(tablesTableDef.tn,
		&table{
			rce: rce,
			tx:  etx.(*transaction),
			def: tablesTableDef,
		})
}

func (rce *rowColsEngine) LookupTable(ctx context.Context, etx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	mid, err := util.LookupTable(ctx, rce, etx, tn)
	if err != nil {
		return nil, err
	} else if mid == 0 {
		return nil, fmt.Errorf("rowcols: table %s not found", tn)
	}

	def, ok := rce.tableDefs[mid]
	if !ok {
		panic(fmt.Sprintf("rowcols: table %s missing table definition", tn))
	}

	return &table{
		rce: rce,
		tx:  etx.(*transaction),
		def: def,
	}, nil
}

func (rce *rowColsEngine) CreateTable(ctx context.Context, etx engine.Transaction,
	tn sql.TableName, cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	if primary == nil {
		rowID := sql.ID("rowid")

		for _, col := range cols {
			if col == rowID {
				return fmt.Errorf(
					"rowcols: unable to add %s column for table %s missing primary key",
					rowID, tn)
			}
		}

		primary = []engine.ColumnKey{
			engine.MakeColumnKey(len(cols), false),
		}
		cols = append(cols, rowID)
		colTypes = append(colTypes, sql.ColumnType{
			Type:    sql.IntegerType,
			Size:    8,
			NotNull: true,
			Default: &expr.Call{Name: sql.ID("unique_rowid")},
		})
	}

	mid, err := util.CreateTable(ctx, rce, etx, tn, ifNotExists)
	if err != nil {
		return err
	}
	if mid == 0 {
		return nil
	}

	rce.tableDefs[mid] = makeTableDef(tn, cols, colTypes, primary, mid)
	return nil
}

func (rce *rowColsEngine) DropTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	return util.DropTable(ctx, rce, etx, tn, ifExists)
}

func (rce *rowColsEngine) MakeIndexesTable(etx engine.Transaction) *util.TypedTable {
	return util.MakeTypedTable(indexesTableDef.tn,
		&table{
			rce: rce,
			tx:  etx.(*transaction),
			def: indexesTableDef,
		})
}

func (rce *rowColsEngine) CreateIndex(ctx context.Context, etx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, unique bool, keys []engine.ColumnKey,
	ifNotExists bool) error {

	return util.CreateIndex(ctx, rce, etx, idxname, tn, ifNotExists)
}

func (rce *rowColsEngine) DropIndex(ctx context.Context, etx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, ifExists bool) error {

	return util.DropIndex(ctx, rce, etx, idxname, tn, ifExists)
}

func (rce *rowColsEngine) Begin(sesid uint64) engine.Transaction {
	rce.mutex.Lock()
	defer rce.mutex.Unlock()
	return &transaction{
		rce:  rce,
		tree: rce.tree.Clone(),
		ver:  rce.ver,
	}
}

func (rce *rowColsEngine) RowItem(ri rowItem) error {
	if ri.ver > rce.ver {
		rce.ver = ri.ver
	}
	rce.tree.ReplaceOrInsert(ri)
	return nil
}

func (rce *rowColsEngine) commit(ctx context.Context, txVer uint64, delta *btree.BTree) error {
	rce.commitMutex.Lock()
	defer rce.commitMutex.Unlock()

	rce.mutex.Lock()
	tree := rce.tree.Clone()
	rce.mutex.Unlock()

	ver := rce.ver + 1
	buf := EncodeUint32([]byte{commitRecordType}, 0) // Reserve space for length.
	buf = EncodeUint64(buf, ver)

	var err error
	delta.Ascend(
		func(item btree.Item) bool {
			txri := item.(rowItem)
			cur := tree.Get(txri)
			if cur == nil {
				if !txri.deleted {
					txri.ver = ver
					tree.ReplaceOrInsert(txri)
					buf = encodeRowItem(buf, txri)
				}
			} else {
				ri := cur.(rowItem)
				if ri.ver > txVer {
					err = errors.New("rowcols: write conflict committing transaction")
					return false
				}
				if !txri.deleted || !ri.deleted {
					txri.ver = ver
					tree.ReplaceOrInsert(txri)
					buf = encodeRowItem(buf, txri)
				}
			}
			return true
		})
	if err != nil {
		return err
	}

	if err := rce.wal.writeCommit(buf); err != nil {
		return err
	}

	rce.mutex.Lock()
	rce.tree = tree
	rce.ver = ver
	rce.mutex.Unlock()

	return nil
}

func (rce *rowColsEngine) ListDatabases(ctx context.Context,
	tx engine.Transaction) ([]sql.Identifier, error) {

	var dbnames []sql.Identifier
	for dbname := range rce.databases {
		dbnames = append(dbnames, dbname)
	}
	return dbnames, nil
}

func (rce *rowColsEngine) ListSchemas(ctx context.Context, etx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	return util.ListSchemas(ctx, rce, etx, dbname)
}

func (rce *rowColsEngine) ListTables(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	return util.ListTables(ctx, rce, etx, sn)
}

func (rctx *transaction) Commit(ctx context.Context) error {
	if rctx.rce == nil {
		return errTransactionComplete
	}

	var err error
	if rctx.delta != nil {
		err = rctx.rce.commit(ctx, rctx.ver, rctx.delta)
	}

	rctx.rce = nil
	rctx.tree = nil
	rctx.delta = nil
	return err
}

func (rctx *transaction) Rollback() error {
	if rctx.rce == nil {
		return errTransactionComplete
	}

	rctx.rce = nil
	rctx.tree = nil
	rctx.delta = nil
	return nil
}

func (_ *transaction) NextStmt() {}

func (rctx *transaction) forWrite() {
	if rctx.delta == nil {
		rctx.delta = btree.New(16)
	}
}

func makeTableDef(tn sql.TableName, cols []sql.Identifier, colTypes []sql.ColumnType,
	primary []engine.ColumnKey, mid uint64) *tableDef {

	if len(primary) == 0 {
		panic(fmt.Sprintf("rowcols: table %s: missing required primary key", tn))
	}
	if len(primary) > 32 {
		panic(fmt.Sprintf("rowcols: table %s: primary key with too many columns", tn))
	}

	def := tableDef{
		tn:          tn,
		columns:     cols,
		columnTypes: colTypes,
		primary:     primary,
		mid:         mid,
	}

	def.reverse = 0
	def.rowCols = make([]int, len(cols))
	vn := len(primary)
	for cn := range cols {
		isValue := true

		for kn, ck := range primary {
			if ck.Number() == cn {
				def.rowCols[kn] = cn
				if ck.Reverse() {
					def.reverse |= 1 << kn
				}
				isValue = false
				break
			}
		}

		if isValue {
			def.rowCols[vn] = cn
			vn += 1
		}
	}

	return &def
}

func (def *tableDef) toItem(row []sql.Value, ver uint64, deleted bool) btree.Item {
	ri := rowItem{
		mid:        def.mid,
		ver:        ver,
		reverse:    def.reverse,
		deleted:    deleted,
		numKeyCols: uint8(len(def.primary)),
	}

	if row != nil {
		ri.row = make([]sql.Value, len(def.columns))
		for rdx := range def.rowCols {
			ri.row[rdx] = row[def.rowCols[rdx]]
		}
	}

	return ri
}

func (def *tableDef) toRow(ri rowItem) []sql.Value {
	if ri.row == nil {
		return nil
	}
	row := make([]sql.Value, len(def.columns))
	for rdx := range def.rowCols {
		row[def.rowCols[rdx]] = ri.row[rdx]
	}
	return row
}

func (ri rowItem) compare(ri2 rowItem) int {
	if ri.mid < ri2.mid {
		return -1
	} else if ri.mid > ri2.mid {
		return 1
	} else if ri2.row == nil {
		if ri.row == nil {
			return 0
		}
		return -1
	} else if ri.row == nil {
		return -1
	}

	for kdx := uint8(0); kdx < ri.numKeyCols; kdx += 1 {
		cmp := sql.Compare(ri.row[kdx], ri2.row[kdx])
		if cmp == 0 {
			continue
		}
		if ri.reverse&(1<<kdx) != 0 {
			return -1 * cmp
		} else {
			return cmp
		}
	}

	return 0
}

func (ri rowItem) Less(item btree.Item) bool {
	return ri.compare(item.(rowItem)) < 0
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

func (bt *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (engine.Rows, error) {
	br := &rows{
		tbl: bt,
		idx: 0,
	}

	var maxItem btree.Item
	if maxRow != nil {
		maxItem = bt.def.toItem(maxRow, 0, false)
	}

	if bt.tx.delta == nil {
		bt.tx.tree.AscendGreaterOrEqual(bt.def.toItem(minRow, 0, false),
			func(item btree.Item) bool {
				if maxItem != nil && maxItem.Less(item) {
					return false
				}
				ri := item.(rowItem)
				if ri.mid != bt.def.mid {
					return false
				}
				if !ri.deleted {
					br.rows = append(br.rows, bt.def.toRow(ri))
				}
				return true
			})
		return br, nil
	}

	var deltaRows []rowItem
	bt.tx.delta.AscendGreaterOrEqual(bt.def.toItem(minRow, 0, false),
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.mid != bt.def.mid {
				return false
			}
			deltaRows = append(deltaRows, ri)
			return true
		})

	bt.tx.tree.AscendGreaterOrEqual(bt.def.toItem(minRow, 0, false),
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.mid != bt.def.mid {
				return false
			}

			for len(deltaRows) > 0 {
				cmp := ri.compare(deltaRows[0])
				if cmp < 0 {
					break
				} else if cmp > 0 {
					if !deltaRows[0].deleted {
						br.rows = append(br.rows, bt.def.toRow(deltaRows[0]))
					}
					deltaRows = deltaRows[1:]
				} else {
					if !deltaRows[0].deleted {
						// Must be an update.
						br.rows = append(br.rows, bt.def.toRow(deltaRows[0]))
						deltaRows = deltaRows[1:]
					}
					return true
				}
			}

			if !ri.deleted {
				br.rows = append(br.rows, bt.def.toRow(ri))
			}
			return true
		})

	for _, ri := range deltaRows {
		if !ri.deleted {
			br.rows = append(br.rows, bt.def.toRow(ri))
		}
	}

	return br, nil
}

func (bt *table) Insert(ctx context.Context, row []sql.Value) error {
	bt.tx.forWrite()

	ri := bt.def.toItem(row, 0, false)
	if item := bt.tx.delta.Get(ri); item != nil {
		if !(item.(rowItem)).deleted {
			return fmt.Errorf("rowcols: %s: existing row with duplicate primary key", bt.def.tn)
		}
	} else if item := bt.tx.tree.Get(ri); item != nil && !(item.(rowItem)).deleted {
		return fmt.Errorf("rowcols: %s: existing row with duplicate primary key", bt.def.tn)
	}

	bt.tx.delta.ReplaceOrInsert(ri)
	return nil
}

func (br *rows) Columns() []sql.Identifier {
	return br.tbl.def.columns
}

func (br *rows) Close() error {
	br.tbl = nil
	br.rows = nil
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
		panic(fmt.Sprintf("rowcols: table %s no row to delete", br.tbl.def.tn))
	}

	br.tbl.tx.delta.ReplaceOrInsert(br.tbl.def.toItem(br.rows[br.idx-1], 0, true))
	return nil
}

func (br *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	br.tbl.tx.forWrite()

	if br.idx == 0 {
		panic(fmt.Sprintf("rowcols: table %s no row to update", br.tbl.def.tn))
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

	for _, update := range updates {
		br.rows[br.idx-1][update.Index] = update.Value
	}
	br.tbl.tx.delta.ReplaceOrInsert(br.tbl.def.toItem(br.rows[br.idx-1], 0, false))
	return nil
}
