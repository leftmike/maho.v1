package memrows

/*
- row index is fixed and never changes for the life of a row
- keep track of deleted rows and reuse them
- cleanup old versions and old rows: vacuum
- snapshots
*/

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/service"
	"github.com/leftmike/maho/engine/virtual"
	"github.com/leftmike/maho/sql"
)

type memrowsEngine struct {
	txService service.TransactionService
	mutex     sync.RWMutex
	databases map[sql.Identifier]*database
}

type database struct {
	mutex   sync.RWMutex
	name    sql.Identifier
	schemas map[sql.SchemaName]*schemaImpl
	tables  map[sql.TableName]*tableImpl
	version version // current version of the database
	nextTID tid
}

type schemaImpl struct {
	createdVersion version
	droppedVersion version
	dropped        bool
	previous       *schemaImpl
}

type tcontext struct {
	tx      *service.Transaction
	version version
	tid     tid
	cid     cid
	schemas map[sql.SchemaName]*schema
	tables  map[sql.TableName]*table
}

type schema struct {
	schema *schemaImpl

	// True if and only if the schema existed before the transaction started.
	dropped bool

	// True if the schema was created by the transaction. If a schema with the same name existed
	// before the transaction started, then it must have been dropped first and dropped will
	// be true. Otherwise, a schema with the same name must not have existed when the transaction
	// started.
	created bool
}

type table struct {
	tctx         *tcontext
	tn           sql.TableName
	modifyLock   bool
	table        *tableImpl
	modifiedRows []int // indexes of modified rows

	// True if and only if the table existed before the transaction started.
	dropped bool

	// True if the table was created by the transaction. If a table with the same name existed
	// before the transaction started, then it must have been dropped first and dropped will
	// be true. Otherwise, a table with the same name must not have existed when the transaction
	// started.
	created bool
}

type rows struct {
	table   *table
	index   int
	haveRow bool
}

func NewEngine(dataDir string) engine.Engine {
	me := &memrowsEngine{
		databases: map[sql.Identifier]*database{},
	}
	ve := virtual.NewEngine(me)
	me.txService.Init(ve)
	return ve
}

func (_ *memrowsEngine) CreateSystemTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("memrows: use virtual engine with memrows engine")
}

func (_ *memrowsEngine) CreateInfoTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic("memrows: use virtual engine with memrows engine")
}

func (me *memrowsEngine) CreateDatabase(dbname sql.Identifier, options engine.Options) error {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	if _, ok := me.databases[dbname]; ok {
		return fmt.Errorf("memrows: database %s already exists", dbname)
	}
	me.databases[dbname] = &database{
		name: dbname,
		schemas: map[sql.SchemaName]*schemaImpl{
			sql.SchemaName{dbname, sql.PUBLIC}: &schemaImpl{},
		},
		tables: map[sql.TableName]*tableImpl{},
	}
	return nil
}

func (me *memrowsEngine) DropDatabase(dbname sql.Identifier, ifExists bool,
	options engine.Options) error {

	me.mutex.Lock()
	defer me.mutex.Unlock()

	_, ok := me.databases[dbname]
	if !ok {
		if ifExists {
			return nil
		}
		return fmt.Errorf("memrows: database %s does not exist", dbname)
	}
	delete(me.databases, dbname)
	return nil // XXX: don't return until all transactions are done
}

func (me *memrowsEngine) CreateSchema(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) error {

	me.mutex.Lock()
	defer me.mutex.Unlock()

	mdb, ok := me.databases[sn.Database]
	if !ok {
		return fmt.Errorf("memrows: database %s not found", sn.Database)
	}
	return mdb.createSchema(ctx, tx, sn)
}

func (me *memrowsEngine) DropSchema(ctx context.Context, tx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	me.mutex.Lock()
	defer me.mutex.Unlock()

	mdb, ok := me.databases[sn.Database]
	if !ok {
		return fmt.Errorf("memrows: database %s not found", sn.Database)
	}
	return mdb.dropSchema(ctx, tx, sn, ifExists)
}

func (me *memrowsEngine) LookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	me.mutex.RLock()
	defer me.mutex.RUnlock()

	mdb, ok := me.databases[tn.Database]
	if !ok {
		return nil, fmt.Errorf("memrows: database %s not found", tn.Database)
	}
	return mdb.lookupTable(ctx, tx, tn)
}

func (me *memrowsEngine) CreateTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType) error {

	me.mutex.Lock()
	defer me.mutex.Unlock()

	mdb, ok := me.databases[tn.Database]
	if !ok {
		return fmt.Errorf("memrows: database %s not found", tn.Database)
	}
	return mdb.createTable(ctx, tx, tn, cols, colTypes)
}

func (me *memrowsEngine) DropTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	me.mutex.Lock()
	defer me.mutex.Unlock()

	mdb, ok := me.databases[tn.Database]
	if !ok {
		return fmt.Errorf("memrows: database %s not found", tn.Database)
	}
	return mdb.dropTable(ctx, tx, tn, ifExists)
}

func (me *memrowsEngine) Begin(sid uint64) engine.Transaction {
	return me.txService.Begin(sid)
}

func (_ *memrowsEngine) IsTransactional() bool {
	return true
}

func (me *memrowsEngine) ListDatabases(ctx context.Context,
	tx engine.Transaction) ([]sql.Identifier, error) {

	me.mutex.RLock()
	defer me.mutex.RUnlock()

	var dbnames []sql.Identifier
	for dbname := range me.databases {
		dbnames = append(dbnames, dbname)
	}
	return dbnames, nil
}

func (me *memrowsEngine) ListSchemas(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	me.mutex.RLock()
	mdb, ok := me.databases[dbname]
	me.mutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("memrows: database %s not found", dbname)
	}
	return mdb.listSchemas(ctx, tx)
}

func (me *memrowsEngine) ListTables(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	me.mutex.RLock()
	mdb, ok := me.databases[sn.Database]
	me.mutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("memrows: database %s not found", sn.Database)
	}
	return mdb.listTables(ctx, tx, sn)
}

func (mdb *database) createSchema(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) error {

	tctx := service.GetTxContext(tx, mdb).(*tcontext)
	err := tctx.tx.LockSchema(ctx, sn, service.EXCLUSIVE)
	if err != nil {
		return err
	}

	if sc, ok := tctx.schemas[sn]; ok {
		if sc.dropped && !sc.created {
			sc.created = true
			sc.schema = &schemaImpl{}
			return nil
		}
		return fmt.Errorf("memrows: schema %s already exists", sn)
	}

	mdb.mutex.Lock()
	si, ok := mdb.schemas[sn]
	mdb.mutex.Unlock()

	if ok {
		if si.createdVersion <= tctx.version && !si.dropped {
			return fmt.Errorf("memrows: schema %s already exists", sn)
		} else if si.createdVersion > tctx.version ||
			(si.dropped && si.droppedVersion > tctx.version) {

			return fmt.Errorf("memrows: schema %s conflicting change", sn)
		}
	}

	tctx.schemas[sn] = &schema{
		created: true,
		schema:  &schemaImpl{},
	}
	return nil
}

func (mdb *database) dropSchema(ctx context.Context, tx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	tctx := service.GetTxContext(tx, mdb).(*tcontext)
	err := tctx.tx.LockSchema(ctx, sn, service.EXCLUSIVE)
	if err != nil {
		return err
	}

	tblnames, err := mdb.listTables(ctx, tx, sn)
	if err != nil {
		return err
	}
	if len(tblnames) != 0 {
		return fmt.Errorf("memrows: schemas %s is not empty", sn)
	}

	if sc, ok := tctx.schemas[sn]; ok {
		if sc.created {
			// The schema was created in this transaction.
			if sc.dropped {
				// But there was a previous schema which was dropped first.
				sc.created = false
				sc.schema = nil
			} else {
				// No previous schema, so just forget about this one.
				delete(tctx.schemas, sn)
			}
			return nil
		} else if sc.dropped {
			if ifExists {
				return nil
			}
			return fmt.Errorf("memrows: schema %s does not exist", sn)
		}
		sc.dropped = true
		sc.schema = nil
		return nil
	}

	mdb.mutex.Lock()
	si, ok := mdb.schemas[sn]
	mdb.mutex.Unlock()

	if !ok || (si.dropped && si.droppedVersion <= tctx.version) {
		if ifExists {
			return nil
		}
		return fmt.Errorf("memrows: schema %s does not exist", sn)
	} else if si.createdVersion > tctx.version || (si.dropped && si.droppedVersion > tctx.version) {
		return fmt.Errorf("memrows: schema %s conflicting change", sn)
	}

	tctx.schemas[sn] = &schema{
		dropped: true,
	}
	return nil
}

func visibleTableVersion(ti *tableImpl, v version) *tableImpl {
	for ti != nil {
		if v >= ti.createdVersion && (!ti.dropped || v < ti.droppedVersion) {
			break
		}
		ti = ti.previous
	}
	return ti
}

func (mdb *database) lookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	tctx := service.GetTxContext(tx, mdb).(*tcontext)
	tbl, ok := tctx.tables[tn]
	if ok {
		if tbl.dropped && !tbl.created {
			return nil, fmt.Errorf("memrows: table %s not found", tn)
		}
		return tbl, nil
	}

	err := tctx.tx.LockTable(ctx, tn, service.ACCESS)
	if err != nil {
		return nil, err
	}

	mdb.mutex.RLock()
	ti, _ := mdb.tables[tn]
	mdb.mutex.RUnlock()

	ti = visibleTableVersion(ti, tctx.version)
	if ti == nil {
		return nil, fmt.Errorf("memrows: table %s not found", tn)
	}

	tbl = &table{
		tctx:  tctx,
		tn:    tn,
		table: ti,
	}
	tctx.tables[tn] = tbl
	return tbl, nil
}

func visibleSchemaVersion(si *schemaImpl, v version) *schemaImpl {
	for si != nil {
		if v >= si.createdVersion && (!si.dropped || v < si.droppedVersion) {
			break
		}
		si = si.previous
	}
	return si
}

func (mdb *database) schemaVisible(tctx *tcontext, sn sql.SchemaName) bool {

	if sc, ok := tctx.schemas[sn]; ok {
		if sc.dropped && !sc.created {
			return false
		}
		return true
	}

	mdb.mutex.Lock()
	si, _ := mdb.schemas[sn]
	mdb.mutex.Unlock()

	return visibleSchemaVersion(si, tctx.version) != nil
}

func (mdb *database) createTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType) error {

	tctx := service.GetTxContext(tx, mdb).(*tcontext)
	err := tctx.tx.LockSchema(ctx, tn.SchemaName(), service.ACCESS)
	if err != nil {
		return err
	}
	err = tctx.tx.LockTable(ctx, tn, service.EXCLUSIVE)
	if err != nil {
		return err
	}

	if tbl, ok := tctx.tables[tn]; ok {
		if tbl.dropped && !tbl.created {
			tbl.created = true
			tbl.table = &tableImpl{
				tn:          tn,
				columns:     cols,
				columnTypes: colTypes,
			}
			return nil
		}
		return fmt.Errorf("memrows: table %s already exists", tn)
	}

	mdb.mutex.Lock()
	ti, ok := mdb.tables[tn]
	mdb.mutex.Unlock()

	if ok {
		if ti.createdVersion <= tctx.version && !ti.dropped {
			return fmt.Errorf("memrows: table %s already exists", tn)
		} else if ti.createdVersion > tctx.version ||
			(ti.dropped && ti.droppedVersion > tctx.version) {

			return fmt.Errorf("memrows: table %s conflicting change", tn)
		}
	}

	sn := tn.SchemaName()
	if !mdb.schemaVisible(tctx, sn) {
		return fmt.Errorf("memrows: schema %s not found", sn)
	}

	tctx.tables[tn] = &table{
		tctx:    tctx,
		tn:      tn,
		created: true,
		table: &tableImpl{
			tn:          tn,
			columns:     cols,
			columnTypes: colTypes,
		},
	}
	return nil
}

func (mdb *database) dropTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	tctx := service.GetTxContext(tx, mdb).(*tcontext)
	err := tctx.tx.LockSchema(ctx, tn.SchemaName(), service.ACCESS)
	if err != nil {
		return err
	}
	err = tctx.tx.LockTable(ctx, tn, service.EXCLUSIVE)
	if err != nil {
		return err
	}

	if tbl, ok := tctx.tables[tn]; ok {
		if tbl.created {
			// The table was created in this transaction.
			if tbl.dropped {
				// But there was a previous table which was dropped first.
				tbl.created = false
				tbl.table = nil
			} else {
				// No previous table, so just forget about this one.
				delete(tctx.tables, tn)
			}
			return nil
		} else if tbl.dropped {
			if ifExists {
				return nil
			}
			return fmt.Errorf("memrows: table %s does not exist", tn)
		}
		tbl.dropped = true
		tbl.table = nil
		return nil
	}

	mdb.mutex.Lock()
	ti, ok := mdb.tables[tn]
	mdb.mutex.Unlock()

	if !ok || (ti.dropped && ti.droppedVersion <= tctx.version) {
		if ifExists {
			return nil
		}
		return fmt.Errorf("memrows: table %s does not exist", tn)
	} else if ti.createdVersion > tctx.version || (ti.dropped && ti.droppedVersion > tctx.version) {
		return fmt.Errorf("memrows: table %s conflicting change", tn)
	}

	tctx.tables[tn] = &table{
		tctx:    tctx,
		tn:      tn,
		dropped: true,
	}
	return nil
}

func (mdb *database) listSchemas(ctx context.Context, tx engine.Transaction) ([]sql.Identifier,
	error) {

	var scnames []sql.Identifier

	tctx := service.GetTxContext(tx, mdb).(*tcontext)
	for sn, sc := range tctx.schemas {
		if !sc.dropped {
			scnames = append(scnames, sn.Schema)
		}
	}

	mdb.mutex.RLock()
	defer mdb.mutex.RUnlock()

	for sn, si := range mdb.schemas {
		if _, ok := tctx.schemas[sn]; ok {
			continue
		}
		if visibleSchemaVersion(si, tctx.version) != nil {
			scnames = append(scnames, sn.Schema)
		}
	}

	return scnames, nil
}

func (mdb *database) listTables(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	var tblnames []sql.Identifier

	tctx := service.GetTxContext(tx, mdb).(*tcontext)
	for tn, tbl := range tctx.tables {
		if !tbl.dropped {
			tblnames = append(tblnames, tn.Table)
		}
	}

	mdb.mutex.RLock()
	defer mdb.mutex.RUnlock()

	for tn, ti := range mdb.tables {
		if _, ok := tctx.tables[tn]; ok {
			continue
		}
		if tn.SchemaName() != sn {
			continue
		}
		if visibleTableVersion(ti, tctx.version) != nil {
			tblnames = append(tblnames, tn.Table)
		}
	}
	return tblnames, nil
}

func (mdb *database) Begin(tx *service.Transaction) interface{} {
	mdb.mutex.Lock()
	defer mdb.mutex.Unlock()

	mdb.nextTID += 1
	return &tcontext{
		tx:      tx,
		version: mdb.version,
		tid:     mdb.nextTID - 1,
		schemas: map[sql.SchemaName]*schema{},
		tables:  map[sql.TableName]*table{},
	}
}

func (mdb *database) Commit(ctx context.Context, tx interface{}) error {
	tctx := tx.(*tcontext)

	for _, tbl := range tctx.tables {
		if tbl.table != nil {
			err := tbl.table.checkRows(tctx.tid, tbl.modifiedRows)
			if err != nil {
				return err
			}
		}
	}

	mdb.mutex.Lock()
	defer mdb.mutex.Unlock()

	mdb.version += 1
	v := mdb.version
	for _, tbl := range tctx.tables {
		if tbl.table != nil {
			tbl.table.commitRows(v, tbl.modifiedRows)
		}
	}

	for _, tbl := range tctx.tables {
		if tbl.dropped {
			ti := mdb.tables[tbl.tn]
			ti.droppedVersion = v
			ti.dropped = true
		}
		if tbl.created {
			ti := tbl.table
			ti.createdVersion = v
			pti, _ := mdb.tables[tbl.tn]
			ti.previous = pti
			mdb.tables[tbl.tn] = ti
		}
		tbl.tctx = nil
	}

	for sn, sc := range tctx.schemas {
		if sc.dropped {
			si := mdb.schemas[sn]
			si.droppedVersion = v
			si.dropped = true
		}
		if sc.created {
			si := sc.schema
			si.createdVersion = v
			psi, _ := mdb.schemas[sn]
			si.previous = psi
			mdb.schemas[sn] = si
		}
	}
	return nil
}

func (mdb *database) Rollback(tx interface{}) error {
	tctx := tx.(*tcontext)

	for _, tbl := range tctx.tables {
		if tbl.table != nil {
			err := tbl.table.checkRows(tctx.tid, tbl.modifiedRows)
			if err != nil {
				return err
			}
		}
	}

	for _, tbl := range tctx.tables {
		if tbl.table != nil {
			tbl.table.rollbackRows(tbl.modifiedRows)
		}
		tbl.tctx = nil
	}
	return nil
}

func (mdb *database) NextStmt(tx interface{}) {
	tctx := tx.(*tcontext)
	tctx.cid += 1
}

func (mt *table) Columns(ctx context.Context) []sql.Identifier {
	return mt.table.getColumns(mt.tctx)
}

func (mt *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return mt.table.getColumnTypes(mt.tctx)
}

func (mt *table) Rows(ctx context.Context) (engine.Rows, error) {
	return &rows{table: mt}, nil
}

func (mt *table) Insert(ctx context.Context, row []sql.Value) error {
	if !mt.modifyLock {
		err := mt.tctx.tx.LockTable(ctx, mt.tn, service.ROW_MODIFY)
		if err != nil {
			return err
		}
		mt.modifyLock = true
	}

	idx, err := mt.table.insert(mt.tctx, row)
	if err != nil {
		return err
	}
	mt.modifiedRows = append(mt.modifiedRows, idx)
	return nil
}

func (mr *rows) Columns() []sql.Identifier {
	return mr.table.table.getColumns(mr.table.tctx)
}

func (mr *rows) Close() error {
	mr.index = math.MaxInt64
	mr.haveRow = false
	return nil
}

func (mr *rows) Next(ctx context.Context, dest []sql.Value) error {
	var err error
	mr.index, err = mr.table.table.next(mr.table.tctx, dest, mr.index)
	if err != nil {
		mr.haveRow = false
		return err
	}
	mr.haveRow = true
	return nil
}

func (mr *rows) Delete(ctx context.Context) error {
	if !mr.haveRow {
		return fmt.Errorf("memrows: table %s no row to delete", mr.table.tn)
	}
	if !mr.table.modifyLock {
		err := mr.table.tctx.tx.LockTable(ctx, mr.table.tn, service.ROW_MODIFY)
		if err != nil {
			return err
		}
		mr.table.modifyLock = true
	}

	mr.haveRow = false
	err := mr.table.table.deleteRow(mr.table.tctx, mr.index-1)
	if err != nil {
		return err
	}
	mr.table.modifiedRows = append(mr.table.modifiedRows, mr.index-1)
	return nil
}

func (mr *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	if !mr.haveRow {
		return fmt.Errorf("memrows: table %s no row to update", mr.table.tn)
	}
	if !mr.table.modifyLock {
		err := mr.table.tctx.tx.LockTable(ctx, mr.table.tn, service.ROW_MODIFY)
		if err != nil {
			return err
		}
		mr.table.modifyLock = true
	}

	err := mr.table.table.updateRow(mr.table.tctx, updates, mr.index-1)
	if err != nil {
		return err
	}
	mr.table.modifiedRows = append(mr.table.modifiedRows, mr.index-1)
	return nil
}
