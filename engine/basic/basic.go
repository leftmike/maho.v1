package basic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/btree"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/typedtbl"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
)

const (
	schemasMID = 1
	tablesMID  = 2
)

var (
	notImplemented         = errors.New("basic: not implemented")
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
)

type basicEngine struct {
	mutex       sync.Mutex
	databases   map[sql.Identifier]struct{}
	definitions map[uint64]*tableDef
	tree        *btree.BTree
	lastMID     uint64
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
	row []sql.Value
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
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

type schemaRow struct {
	Database string
	Schema   string
	Tables   int64
}

func (be *basicEngine) makeSchemasTable(tx *transaction) *typedtbl.Table {
	return typedtbl.MakeTable(schemasTableDef.tn,
		&table{
			be:  be,
			tx:  tx,
			def: schemasTableDef,
		})
}

func (be *basicEngine) CreateSchema(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) error {

	_, ok := be.databases[sn.Database]
	if !ok {
		return fmt.Errorf("basic: database %s not found", sn.Database)
	}

	ttbl := be.makeSchemasTable(etx.(*transaction))
	return ttbl.Insert(ctx,
		schemaRow{
			Database: sn.Database.String(),
			Schema:   sn.Schema.String(),
			Tables:   0,
		})
}

func (be *basicEngine) DropSchema(ctx context.Context, etx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	ttbl := be.makeSchemasTable(etx.(*transaction))
	rows, err := ttbl.Seek(ctx,
		[]sql.Value{sql.StringValue(sn.Database.String()), sql.StringValue(sn.Schema.String())})
	if err != nil {
		return err
	}
	defer rows.Close()

	var sr schemaRow
	err = rows.Next(ctx, &sr)
	if err == io.EOF {
		if ifExists {
			return nil
		}
		return fmt.Errorf("basic: schema %s not found", sn)
	} else if err != nil {
		return err
	}

	if sr.Database != sn.Database.String() || sr.Schema != sn.Schema.String() {
		if ifExists {
			return nil
		}
		return fmt.Errorf("basic: schema %s not found", sn)
	}
	if sr.Tables > 0 {
		return fmt.Errorf("basic: schema %s is not empty", sn)
	}
	return rows.Delete(ctx)
}

func (be *basicEngine) updateSchema(ctx context.Context, tx *transaction, sn sql.SchemaName,
	delta int64) error {

	ttbl := be.makeSchemasTable(tx)
	rows, err := ttbl.Seek(ctx,
		[]sql.Value{sql.StringValue(sn.Database.String()), sql.StringValue(sn.Schema.String())})
	if err != nil {
		return err
	}
	defer rows.Close()

	var sr schemaRow
	err = rows.Next(ctx, &sr)
	if err == io.EOF {
		return fmt.Errorf("basic: schema %s not found", sn)
	} else if err != nil {
		return err
	}

	if sr.Database != sn.Database.String() || sr.Schema != sn.Schema.String() {
		return fmt.Errorf("basic: schema %s not found", sn)
	}
	return rows.Update(ctx,
		struct {
			Tables int64
		}{sr.Tables + delta})
}

type tableRow struct {
	Database string
	Schema   string
	Table    string
	MID      int64
}

func (be *basicEngine) makeTablesTable(tx *transaction) *typedtbl.Table {
	return typedtbl.MakeTable(tablesTableDef.tn,
		&table{
			be:  be,
			tx:  tx,
			def: tablesTableDef,
		})
}

func (be *basicEngine) LookupTable(ctx context.Context, etx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	tx := etx.(*transaction)
	ttbl := be.makeTablesTable(tx)
	rows, err := ttbl.Seek(ctx,
		[]sql.Value{
			sql.StringValue(tn.Database.String()),
			sql.StringValue(tn.Schema.String()),
			sql.StringValue(tn.Table.String()),
		})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tr tableRow
	err = rows.Next(ctx, &tr)
	if err == io.EOF {
		return nil, fmt.Errorf("basic: table %s not found", tn)
	} else if err != nil {
		return nil, err
	}

	if tr.Database != tn.Database.String() || tr.Schema != tn.Schema.String() ||
		tr.Table != tn.Table.String() {

		return nil, fmt.Errorf("basic: table %s not found", tn)
	}

	def, ok := be.definitions[uint64(tr.MID)]
	if !ok {
		panic(fmt.Sprintf("basic: table %s missing table definition", tn))
	}

	return &table{
		be:  be,
		tx:  tx,
		def: def,
	}, nil
}

func (be *basicEngine) CreateTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	tx := etx.(*transaction)
	err := be.updateSchema(ctx, tx, tn.SchemaName(), 1)
	if err != nil {
		return err
	}

	be.lastMID += 1
	ttbl := be.makeTablesTable(tx)
	err = ttbl.Insert(ctx,
		tableRow{
			Database: tn.Database.String(),
			Schema:   tn.Schema.String(),
			Table:    tn.Table.String(),
			MID:      int64(be.lastMID),
		})
	if err != nil {
		return err
	}

	be.definitions[be.lastMID] =
		&tableDef{
			tn:          tn,
			columns:     cols,
			columnTypes: colTypes,
			primary:     primary,
			mid:         be.lastMID,
		}
	return nil
}

func (be *basicEngine) DropTable(ctx context.Context, etx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	tx := etx.(*transaction)
	err := be.updateSchema(ctx, tx, tn.SchemaName(), -1)
	if err != nil {
		return err
	}

	ttbl := be.makeTablesTable(tx)
	rows, err := ttbl.Seek(ctx,
		[]sql.Value{
			sql.StringValue(tn.Database.String()),
			sql.StringValue(tn.Schema.String()),
			sql.StringValue(tn.Table.String()),
		})
	if err != nil {
		return err
	}
	defer rows.Close()

	var tr tableRow
	err = rows.Next(ctx, &tr)
	if err == io.EOF {
		if ifExists {
			return nil
		}
		return fmt.Errorf("basic: table %s not found", tn)
	} else if err != nil {
		return err
	}

	if tr.Database != tn.Database.String() || tr.Schema != tn.Schema.String() ||
		tr.Table != tn.Table.String() {

		if ifExists {
			return nil
		}
		return fmt.Errorf("basic: table %s not found", tn)
	}
	return rows.Delete(ctx)
}

func (be *basicEngine) CreateIndex(ctx context.Context, tx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, unique bool, keys []engine.ColumnKey,
	ifNotExists bool) error {

	// XXX
	return nil
}

func (be *basicEngine) DropIndex(ctx context.Context, tx engine.Transaction, idxname sql.Identifier,
	tn sql.TableName, ifExists bool) error {

	// XXX
	return nil
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

	ttbl := be.makeSchemasTable(etx.(*transaction))
	rows, err := ttbl.Seek(ctx, []sql.Value{sql.StringValue(dbname.String()), sql.StringValue("")})
	if err != nil {
		return nil, err
	}

	var scnames []sql.Identifier
	for {
		var sr schemaRow
		err = rows.Next(ctx, &sr)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if sr.Database != dbname.String() {
			break
		}
		scnames = append(scnames, sql.ID(sr.Schema))
	}
	return scnames, nil
}

func (be *basicEngine) ListTables(ctx context.Context, etx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	tx := etx.(*transaction)
	ttbl := be.makeTablesTable(tx)
	rows, err := ttbl.Seek(ctx,
		[]sql.Value{
			sql.StringValue(sn.Database.String()),
			sql.StringValue(sn.Schema.String()),
			sql.StringValue(""),
		})
	if err != nil {
		return nil, err
	}

	var tblnames []sql.Identifier
	for {
		var tr tableRow
		err = rows.Next(ctx, &tr)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if tr.Database != sn.Database.String() || tr.Schema != sn.Schema.String() {
			break
		}
		tblnames = append(tblnames, sql.ID(tr.Table))
	}
	return tblnames, nil
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
	bt.tx.tree.AscendGreaterOrEqual(midRow{bt.def, row}, br.itemIterator)
	return br, nil
}

func (bt *table) Rows(ctx context.Context) (engine.Rows, error) {
	return bt.Seek(ctx, nil)
}

func (bt *table) Insert(ctx context.Context, row []sql.Value) error {
	bt.tx.forWrite()

	if bt.tx.tree.Has(midRow{bt.def, row}) {
		return fmt.Errorf("basic: %s: existing row with duplicate primary key", bt.def.tn)
	}

	bt.tx.tree.ReplaceOrInsert(midRow{bt.def, append(make([]sql.Value, 0, len(row)), row...)})
	return nil
}

func (br *rows) itemIterator(item btree.Item) bool {
	mr := item.(midRow)
	if mr.def.mid != br.tbl.def.mid {
		return false
	}
	br.rows = append(br.rows, mr.row)
	return true
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
		panic(fmt.Sprintf("basic: table %s no row to delete", br.tbl.def.tn))
	}

	br.tbl.tx.tree.Delete(midRow{br.tbl.def, br.rows[br.idx-1]})
	return nil
}

func (br *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	br.tbl.tx.forWrite()

	if br.idx == 0 {
		// XXX: fix Update and Delete to panic rather than returning an error everywhere
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

	row := append(make([]sql.Value, 0, len(br.rows[br.idx-1])), br.rows[br.idx-1]...)
	for _, update := range updates {
		row[update.Index] = update.Value
	}
	br.tbl.tx.tree.ReplaceOrInsert(midRow{br.tbl.def, row})
	return nil
}
