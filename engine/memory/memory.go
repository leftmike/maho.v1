package memory

/*
- row index is fixed and never changes for the life of a row
- keep track of deleted rows and reuse them
- cleanup old versions and old rows
*/

import (
	"fmt"
	"math"
	"sync"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
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
	version version
	tid     tid
	cid     cid
	tables  map[sql.Identifier]*table
}

type table struct {
	tctx         *tcontext
	table        *tableImpl
	modifiedRows []int // indexes of modified rows
}

type rows struct {
	table   *table
	index   int
	haveRow bool
}

func init() {
	engine.Register("memory", &eng{})
}

func (me *eng) AttachDatabase(name sql.Identifier, path string,
	options engine.Options) (engine.Database, error) {

	return nil, fmt.Errorf("memory: attach database not supported")
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

func (mdb *database) LookupTable(ses db.Session, tx interface{}, tblname sql.Identifier) (db.Table,
	error) {

	tctx := tx.(*tcontext)
	tbl, ok := tctx.tables[tblname]
	if ok {
		return tbl, nil
	}

	mdb.mutex.RLock()
	defer mdb.mutex.RUnlock()

	ti, ok := mdb.tables[tblname]
	if !ok {
		return nil, fmt.Errorf("memory: table %s not found in database %s", tblname, mdb.name)
	}
	tbl = &table{tctx: tctx, table: ti}
	tctx.tables[tblname] = tbl
	return tbl, nil
}

func (mdb *database) CreateTable(ses db.Session, tx interface{}, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []db.ColumnType) error {

	mdb.mutex.Lock()
	defer mdb.mutex.Unlock()

	if _, dup := mdb.tables[tblname]; dup {
		return fmt.Errorf("memory: table %s already exists in database %s", tblname, mdb.name)
	}

	mdb.tables[tblname] = &tableImpl{
		columns:     cols,
		columnTypes: colTypes,
		rows:        nil,
	}
	return nil
}

func (mdb *database) DropTable(ses db.Session, tx interface{}, tblname sql.Identifier,
	exists bool) error {

	mdb.mutex.Lock()
	defer mdb.mutex.Unlock()

	if _, ok := mdb.tables[tblname]; !ok {
		if exists {
			return nil
		}
		return fmt.Errorf("memory: table %s does not exist in database %s", tblname, mdb.name)
	}
	delete(mdb.tables, tblname)
	return nil
}

func (mdb *database) ListTables(ses db.Session, tx interface{}) ([]engine.TableEntry, error) {

	mdb.mutex.RLock()
	defer mdb.mutex.RUnlock()

	var tbls []engine.TableEntry
	for name, _ := range mdb.tables {
		tbls = append(tbls, engine.TableEntry{
			Name: name,
			Type: engine.PhysicalType,
		})
	}
	return tbls, nil
}

func (mdb *database) Begin() interface{} {
	mdb.mutex.Lock()
	defer mdb.mutex.Unlock()

	mdb.nextTID += 1
	return &tcontext{
		version: mdb.version,
		tid:     mdb.nextTID - 1,
		tables:  map[sql.Identifier]*table{},
	}
}

func (mdb *database) Commit(ses db.Session, tx interface{}) error {
	tctx := tx.(*tcontext)

	for _, tbl := range tctx.tables {
		err := tbl.table.checkRows("commit", tctx.tid, tbl.modifiedRows)
		if err != nil {
			return err
		}
	}

	mdb.version += 1
	v := mdb.version
	for _, tbl := range tctx.tables {
		tbl.table.commitRows(v, tbl.modifiedRows)
	}
	return nil
}

func (mdb *database) Rollback(tx interface{}) error {
	tctx := tx.(*tcontext)
	for _, tbl := range tctx.tables {
		err := tbl.table.checkRows("rollback", tctx.tid, tbl.modifiedRows)
		if err != nil {
			return err
		}
	}

	for _, tbl := range tctx.tables {
		tbl.table.rollbackRows(tbl.modifiedRows)
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
		return fmt.Errorf("memory: no row to delete")
	}
	mr.haveRow = false
	err := mr.table.table.delete(mr.table.tctx, mr.index-1)
	if err != nil {
		return err
	}
	mr.table.modifiedRows = append(mr.table.modifiedRows, mr.index-1)
	return nil
}

func (mr *rows) Update(ses db.Session, updates []db.ColumnUpdate) error {
	if !mr.haveRow {
		return fmt.Errorf("memory: no row to update")
	}
	err := mr.table.table.update(mr.table.tctx, updates, mr.index-1)
	if err != nil {
		return err
	}
	mr.table.modifiedRows = append(mr.table.modifiedRows, mr.index-1)
	return nil
}
