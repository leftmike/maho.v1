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
	tables  map[sql.Identifier]*tableImpl
	version version // current version of the database
	nextTID tid
}

type tcontext struct {
	tx      *service.Transaction
	version version
	tid     tid
	cid     cid
	tables  map[sql.Identifier]*table
}

type table struct {
	tctx         *tcontext
	db           *database
	name         sql.Identifier
	modifyLock   bool
	table        *tableImpl
	modifiedRows []int // indexes of modified rows

	/*
		There four possible cases for CreateTable and DropTable within a single transaction
		(for all of them, the transaction has an exclusive lock on the table).
		(1) DropTable: dropped = true
		(2) CreateTable: created = true
		(3) DropTable followed by CreateTable: dropped = true and created = true
		    During Commit, this is handled by always doing drops before creates; this always
		    works because (4) is never visible at Commit time.
		(4) CreateTable followed by DropTable: this is effectively a nop with respect to a
		    committed transaction. It is handled by DropTable detecting the CreateTable and
		    discarding the table context from the transaction.
	*/
	created bool
	dropped bool
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

func (me *memrowsEngine) CreateDatabase(name sql.Identifier, options engine.Options) error {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	if _, ok := me.databases[name]; ok {
		return fmt.Errorf("memrows: database %s already exists", name)
	}
	me.databases[name] = &database{
		name:   name,
		tables: map[sql.Identifier]*tableImpl{},
	}
	return nil
}

func (me *memrowsEngine) DropDatabase(name sql.Identifier, exists bool,
	options engine.Options) error {

	me.mutex.Lock()
	defer me.mutex.Unlock()

	_, ok := me.databases[name]
	if !ok {
		if exists {
			return nil
		}
		return fmt.Errorf("memrows: database %s does not exist", name)
	}
	delete(me.databases, name)
	return nil // XXX: don't return until all transactions are done
}

func (me *memrowsEngine) LookupTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier) (engine.Table, error) {

	me.mutex.RLock()
	defer me.mutex.RUnlock()

	mdb, ok := me.databases[dbname]
	if !ok {
		return nil, fmt.Errorf("memrows: database %s not found", dbname)
	}
	return mdb.lookupTable(ctx, tx, tblname)
}

func (me *memrowsEngine) CreateTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier, cols []sql.Identifier, colTypes []sql.ColumnType) error {

	me.mutex.Lock()
	defer me.mutex.Unlock()

	mdb, ok := me.databases[dbname]
	if !ok {
		return fmt.Errorf("memrows: database %s not found", dbname)
	}
	return mdb.createTable(ctx, tx, tblname, cols, colTypes)
}

func (me *memrowsEngine) DropTable(ctx context.Context, tx engine.Transaction,
	dbname, tblname sql.Identifier, exists bool) error {

	me.mutex.Lock()
	defer me.mutex.Unlock()

	mdb, ok := me.databases[dbname]
	if !ok {
		return fmt.Errorf("memrows: database %s not found", dbname)
	}
	return mdb.dropTable(ctx, tx, tblname, exists)
}

func (me *memrowsEngine) Begin(sid uint64) engine.Transaction {
	return me.txService.Begin(sid)
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

func (me *memrowsEngine) ListTables(ctx context.Context, tx engine.Transaction,
	name sql.Identifier) ([]sql.Identifier, error) {

	me.mutex.RLock()
	mdb, ok := me.databases[name]
	me.mutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("memrows: database %s not found", name)
	}
	return mdb.listTables(ctx, tx)
}

func visibleVersion(ti *tableImpl, v version) *tableImpl {
	for ti != nil {
		if v >= ti.createdVersion && (!ti.dropped || v < ti.droppedVersion) {
			break
		}
		ti = ti.previous
	}
	return ti
}

func (mdb *database) lookupTable(ctx context.Context, tx engine.Transaction,
	tblname sql.Identifier) (engine.Table, error) {

	tctx := service.GetTxContext(tx, mdb).(*tcontext)
	tbl, ok := tctx.tables[tblname]
	if ok {
		if tbl.dropped && !tbl.created {
			return nil, fmt.Errorf("memrows: table %s.%s not found", mdb.name, tblname)
		}
		return tbl, nil
	}

	err := tctx.tx.LockTable(ctx, mdb.name, tblname, service.ACCESS)
	if err != nil {
		return nil, err
	}

	mdb.mutex.RLock()
	ti, _ := mdb.tables[tblname]
	mdb.mutex.RUnlock()

	ti = visibleVersion(ti, tctx.version)
	if ti == nil {
		return nil, fmt.Errorf("memrows: table %s.%s not found", mdb.name, tblname)
	}

	tbl = &table{
		tctx:  tctx,
		db:    mdb,
		name:  tblname,
		table: ti,
	}
	tctx.tables[tblname] = tbl
	return tbl, nil
}

func (mdb *database) createTable(ctx context.Context, tx engine.Transaction, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []sql.ColumnType) error {

	tctx := service.GetTxContext(tx, mdb).(*tcontext)
	err := tctx.tx.LockTable(ctx, mdb.name, tblname, service.EXCLUSIVE)
	if err != nil {
		return err
	}

	if tbl, ok := tctx.tables[tblname]; ok {
		if tbl.dropped && !tbl.created {
			tbl.created = true
			tbl.table = &tableImpl{
				name:        fmt.Sprintf("%s.%s", mdb.name, tblname),
				columns:     cols,
				columnTypes: colTypes,
				rows:        nil,
			}
			return nil
		}
		return fmt.Errorf("memrows: table %s.%s already exists", mdb.name, tblname)
	}

	mdb.mutex.Lock()
	ti, ok := mdb.tables[tblname]
	mdb.mutex.Unlock()

	if ok {
		if ti.createdVersion <= tctx.version && !ti.dropped {
			return fmt.Errorf("memrows: table %s.%s already exists", mdb.name, tblname)
		} else if ti.createdVersion > tctx.version ||
			(ti.dropped && ti.droppedVersion > tctx.version) {

			return fmt.Errorf("memrows: table %s.%s conflicting change", mdb.name, tblname)
		}
	}

	tctx.tables[tblname] = &table{
		tctx:    tctx,
		db:      mdb,
		name:    tblname,
		created: true,
		table: &tableImpl{
			name:        fmt.Sprintf("%s.%s", mdb.name, tblname),
			columns:     cols,
			columnTypes: colTypes,
			rows:        nil,
		},
	}
	return nil
}

func (mdb *database) dropTable(ctx context.Context, tx engine.Transaction, tblname sql.Identifier,
	exists bool) error {

	tctx := service.GetTxContext(tx, mdb).(*tcontext)
	err := tctx.tx.LockTable(ctx, mdb.name, tblname, service.EXCLUSIVE)
	if err != nil {
		return err
	}

	if tbl, ok := tctx.tables[tblname]; ok {
		if tbl.created {
			// The table was created in this transaction so dropping the table now means that
			// even if this transaction committed, the table will never be visible: just go
			// ahead and discard it from the transaction context.
			delete(tctx.tables, tblname)
			return nil
		} else if tbl.dropped {
			if exists {
				return nil
			}
			return fmt.Errorf("memrows: table %s.%s does not exist", mdb.name, tblname)
		}
		tbl.dropped = true
		tbl.table = nil
		return nil
	}

	mdb.mutex.Lock()
	ti, ok := mdb.tables[tblname]
	mdb.mutex.Unlock()

	if !ok || (ti.dropped && ti.droppedVersion <= tctx.version) {
		if exists {
			return nil
		}
		return fmt.Errorf("memrows: table %s.%s does not exist", mdb.name, tblname)
	} else if ti.createdVersion > tctx.version || (ti.dropped && ti.droppedVersion > tctx.version) {
		return fmt.Errorf("memrows: table %s.%s conflicting change", mdb.name, tblname)
	}

	tctx.tables[tblname] = &table{
		tctx:    tctx,
		db:      mdb,
		name:    tblname,
		dropped: true,
	}
	return nil
}

func (mdb *database) listTables(ctx context.Context, tx engine.Transaction) ([]sql.Identifier,
	error) {

	var tblnames []sql.Identifier

	tctx := service.GetTxContext(tx, mdb).(*tcontext)
	for _, tbl := range tctx.tables {
		if !tbl.dropped {
			tblnames = append(tblnames, tbl.name)
		}
	}

	mdb.mutex.RLock()
	defer mdb.mutex.RUnlock()

	for name, ti := range mdb.tables {
		if _, ok := tctx.tables[name]; !ok {
			ti = visibleVersion(ti, tctx.version)
			if ti != nil {
				tblnames = append(tblnames, name)
			}
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
		tables:  map[sql.Identifier]*table{},
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
			ti := mdb.tables[tbl.name]
			ti.droppedVersion = v
			ti.dropped = true
		}
		if tbl.created {
			ti := tbl.table
			ti.createdVersion = v
			pti, _ := mdb.tables[tbl.name]
			ti.previous = pti
			mdb.tables[tbl.name] = ti
		}
		tbl.tctx = nil
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
		err := mt.tctx.tx.LockTable(ctx, mt.db.name, mt.name,
			service.ROW_MODIFY)
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
		return fmt.Errorf("memrows: table %s.%s no row to delete", mr.table.db.name,
			mr.table.name)
	}
	if !mr.table.modifyLock {
		err := mr.table.tctx.tx.LockTable(ctx, mr.table.db.name, mr.table.name, service.ROW_MODIFY)
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
		return fmt.Errorf("memrows: table %s.%s no row to update", mr.table.db.name,
			mr.table.name)
	}
	if !mr.table.modifyLock {
		err := mr.table.tctx.tx.LockTable(ctx, mr.table.db.name, mr.table.name,
			service.ROW_MODIFY)
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
