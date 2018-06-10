package memrows

/*
- row index is fixed and never changes for the life of a row
- keep track of deleted rows and reuse them
- cleanup old versions and old rows: vacuum
- snapshots
*/

import (
	"fmt"
	"math"
	"sync"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/fatlock"
	"github.com/leftmike/maho/sql"
)

type eng struct{}

type database struct {
	mutex   sync.RWMutex
	name    sql.Identifier
	tables  map[sql.Identifier]*tableImpl
	version version // current version of the database
	nextTID tid
}

type tcontext struct {
	locker  fatlock.Locker
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

func init() {
	engine.Register("memrows", &eng{})
}

func (me *eng) AttachDatabase(name sql.Identifier, path string,
	options engine.Options) (engine.Database, error) {

	return nil, fmt.Errorf("memrows: attach database not supported")
}

func (me *eng) CreateDatabase(name sql.Identifier, path string,
	options engine.Options) (engine.Database, error) {

	return &database{
		name:   name,
		tables: map[sql.Identifier]*tableImpl{},
	}, nil
}

func (mdb *database) Message() string {
	return ""
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

func (mdb *database) LookupTable(ses db.Session, tx interface{}, tblname sql.Identifier) (db.Table,
	error) {

	tctx := tx.(*tcontext)
	tbl, ok := tctx.tables[tblname]
	if ok {
		if tbl.dropped && !tbl.created {
			return nil, fmt.Errorf("memrows: table %s not found in database %s", tblname, mdb.name)
		}
		return tbl, nil
	}

	err := fatlock.LockTable(ses, tctx.locker, mdb.name, tblname, fatlock.ACCESS)
	if err != nil {
		return nil, err
	}

	mdb.mutex.RLock()
	ti, _ := mdb.tables[tblname]
	mdb.mutex.RUnlock()

	ti = visibleVersion(ti, tctx.version)
	if ti == nil {
		return nil, fmt.Errorf("memrows: table %s not found in database %s", tblname, mdb.name)
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

func (mdb *database) CreateTable(ses db.Session, tx interface{}, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	tctx := tx.(*tcontext)
	err := fatlock.LockTable(ses, tctx.locker, mdb.name, tblname, fatlock.EXCLUSIVE)
	if err != nil {
		return err
	}

	if tbl, ok := tctx.tables[tblname]; ok {
		if tbl.dropped && !tbl.created {
			tbl.created = true
			tbl.table = &tableImpl{
				columns:     cols,
				columnTypes: colTypes,
				rows:        nil,
			}
			return nil
		}
		return fmt.Errorf("memrows: table %s already exists in database %s", tblname, mdb.name)
	}

	mdb.mutex.Lock()
	ti, ok := mdb.tables[tblname]
	mdb.mutex.Unlock()

	if ok {
		if ti.createdVersion <= tctx.version && !ti.dropped {
			return fmt.Errorf("memrows: table %s already exists in database %s", tblname, mdb.name)
		} else if ti.createdVersion > tctx.version ||
			(ti.dropped && ti.droppedVersion > tctx.version) {

			return fmt.Errorf("memrows: table %s.%s: conflicting change", mdb.name, tblname)
		}
	}

	tctx.tables[tblname] = &table{
		tctx:    tctx,
		db:      mdb,
		name:    tblname,
		created: true,
		table: &tableImpl{
			columns:     cols,
			columnTypes: colTypes,
			rows:        nil,
		},
	}
	return nil
}

func (mdb *database) DropTable(ses db.Session, tx interface{}, tblname sql.Identifier,
	exists bool) error {

	tctx := tx.(*tcontext)
	err := fatlock.LockTable(ses, tctx.locker, mdb.name, tblname, fatlock.EXCLUSIVE)
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
			return fmt.Errorf("memrows: table %s does not exist in database %s", tblname, mdb.name)
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
		return fmt.Errorf("memrows: table %s does not exist in database %s", tblname, mdb.name)
	} else if ti.createdVersion > tctx.version || (ti.dropped && ti.droppedVersion > tctx.version) {
		return fmt.Errorf("memrows: table %s.%s: conflicting change", mdb.name, tblname)
	}

	tctx.tables[tblname] = &table{
		tctx:    tctx,
		db:      mdb,
		name:    tblname,
		dropped: true,
	}
	return nil
}

func (mdb *database) ListTables(ses db.Session, tx interface{}) ([]engine.TableEntry, error) {
	var tbls []engine.TableEntry

	tctx := tx.(*tcontext)
	for _, tbl := range tctx.tables {
		if !tbl.dropped {
			tbls = append(tbls, engine.TableEntry{
				Name: tbl.name,
				Type: engine.PhysicalType,
			})
		}
	}

	mdb.mutex.RLock()
	defer mdb.mutex.RUnlock()

	for name, ti := range mdb.tables {
		if _, ok := tctx.tables[name]; !ok {
			ti = visibleVersion(ti, tctx.version)
			if ti != nil {
				tbls = append(tbls, engine.TableEntry{
					Name: name,
					Type: engine.PhysicalType,
				})
			}
		}
	}
	return tbls, nil
}

func (mdb *database) Begin(lkr fatlock.Locker) interface{} {
	mdb.mutex.Lock()
	defer mdb.mutex.Unlock()

	mdb.nextTID += 1
	return &tcontext{
		locker:  lkr,
		version: mdb.version,
		tid:     mdb.nextTID - 1,
		tables:  map[sql.Identifier]*table{},
	}
}

func (mdb *database) Commit(ses db.Session, tx interface{}) error {
	tctx := tx.(*tcontext)

	for _, tbl := range tctx.tables {
		if tbl.table != nil {
			err := tbl.table.checkRows("commit", tctx.tid, tbl.modifiedRows)
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
			err := tbl.table.checkRows("rollback", tctx.tid, tbl.modifiedRows)
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

func (mt *table) Columns(ses db.Session) []sql.Identifier {
	return mt.table.getColumns(mt.tctx)
}

func (mt *table) ColumnTypes(ses db.Session) []db.ColumnType {
	return mt.table.getColumnTypes(mt.tctx)
}

func (mt *table) Rows(ses db.Session) (db.Rows, error) {
	return &rows{table: mt}, nil
}

func (mt *table) Insert(ses db.Session, row []sql.Value) error {
	if !mt.modifyLock {
		err := fatlock.LockTable(ses, mt.tctx.locker, mt.db.name, mt.name, fatlock.ROW_MODIFY)
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

func (mr *rows) Next(ses db.Session, dest []sql.Value) error {
	var err error
	mr.index, err = mr.table.table.next(mr.table.tctx, dest, mr.index)
	if err != nil {
		mr.haveRow = false
		return err
	}
	mr.haveRow = true
	return nil
}

func (mr *rows) Delete(ses db.Session) error {
	if !mr.haveRow {
		return fmt.Errorf("memrows: no row to delete")
	}
	if !mr.table.modifyLock {
		err := fatlock.LockTable(ses, mr.table.tctx.locker, mr.table.db.name, mr.table.name,
			fatlock.ROW_MODIFY)
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

func (mr *rows) Update(ses db.Session, updates []db.ColumnUpdate) error {
	if !mr.haveRow {
		return fmt.Errorf("memrows: no row to update")
	}
	if !mr.table.modifyLock {
		err := fatlock.LockTable(ses, mr.table.tctx.locker, mr.table.db.name, mr.table.name,
			fatlock.ROW_MODIFY)
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
