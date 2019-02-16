package kvrows

/*
- enhanced kv layer that incorporates mvcc:
-- row version or tid + cid (write intent, tid: pointer to transaction record)
-- transaction record: active, committed, aborted
-- table/<id>/<primary-key><version>:<columns>
-- table/<id>/<primary-key>0:<tid><cid><columns> or table/<id>/<primary-key><tid><cid>:<columns>
-- <columns> in value: non-null and not in key
-- implement simple sql layer in terms of enhanced kv layer
-- allow both layers to be distributed / remote / sharded
-- https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20181209_lazy_txn_record_creation.md

- change database to 1/1/

- <table-id>/<index-id>/<primary-key>:<row>|<protobuf>
- everything is a table; store indexes close to the table
- first user table is at 4096
1/1/<version>:<database-metadata>
2/1/<name>:<tid> // map of table name to id
3/1/<id>:<metadata> // metadata about each table

- tid/iid: 1/1: PRIMARY KEY: version: value is database metadata
- tid/iid: 2/1: PRIMARY KEY: table name; value is tid
- tid/iid: 3/1: PRIMARY KEY: tid; value is table metadata
- tid/iid: 4/1: PRIMARY KEY: sequence name; value is sid
- tid/iid: 5/1: PRIMARY KEY: sid; value is sequence metadata

- things in a table: rows, proposed write, pointer to transaction of proposed write,
  transaction record
- transaction records live close to the first write, which is optimal in distributed store
- rows have versions which need to be last part of key ordered descending
- proposed write + transaction pointer need to sort to top of key
- separating proposed write from transaction pointer means value does not change when write is
  rewritten with commit version
- a table either uses versions, or it does not: tid > 0 && tid < 1000: no version; tid >= 1000
  has version; user tables start at 2000
- Proposal points to a transaction
- ProposedWrite is a write corresponding to the Proposal

- add Transaction protobuf and Proposal protobuf

- need higher level operations on kvrows
*/

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/fatlock"
	"github.com/leftmike/maho/engine/kv"
	"github.com/leftmike/maho/engine/kvrows/encoding"
	"github.com/leftmike/maho/sql"
)

var (
	databaseKey   = []byte("database")
	kvrowsVersion = uint32(1)
)

type Engine struct {
	Engine kv.Engine
}

type database struct {
	lockService   fatlock.LockService
	mutex         sync.Mutex
	metadata      *encoding.DatabaseMetadata
	name          sql.Identifier
	path          string
	db            kv.DB
	tableMetadata map[sql.Identifier]*encoding.TableMetadata
}

type tcontext struct {
	locker fatlock.Locker
	tables map[sql.Identifier]*table
}

type table struct {
	db          *database
	name        sql.Identifier
	metadata    *encoding.TableMetadata
	columns     []sql.Identifier
	columnTypes []sql.ColumnType
	created     bool
	dropped     bool
}

type rows struct {
}

func openDB(db kv.DB, name sql.Identifier) (*encoding.DatabaseMetadata, error) {
	rtx, err := db.ReadTx()
	if err != nil {
		return nil, err
	}
	defer rtx.Discard()

	var md encoding.DatabaseMetadata
	err = rtx.Get(databaseKey,
		func(val []byte) error {
			return nil
		})
	if err != nil {
		return nil, err
	}

	return &md, nil
}

func (e Engine) AttachDatabase(svcs engine.Services, name sql.Identifier, path string,
	options engine.Options) (engine.Database, error) {

	_, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("kvrows: database not found or unavailable at %s", path)
	}

	db, err := e.Engine.Open(path)
	if err != nil {
		return nil, err
	}

	md, err := openDB(db, name)
	if err != nil {
		db.Close() // Ignore any error from close, since we can't do much about it.
		return nil, err
	}

	if md.Version != kvrowsVersion {
		return nil, fmt.Errorf("kvrows: %s: unsupported version: %d", path, md.Version)
	}
	if md.Name != name.String() {
		return nil, fmt.Errorf("kvrows: %s: need %s for database name", path, md.Name)
	}

	md.Opens += 1
	err = setDatabaseMetadata(db, md)
	if err != nil {
		db.Close() // As above, ignore any error from close.
		return nil, err
	}

	return &database{
		lockService:   svcs.LockService(),
		metadata:      md,
		name:          name,
		path:          path,
		db:            db,
		tableMetadata: map[sql.Identifier]*encoding.TableMetadata{},
	}, nil
}

func setDatabaseMetadata(db kv.DB, md *encoding.DatabaseMetadata) error {
	val, err := proto.Marshal(md)
	if err != nil {
		return err
	}

	wtx, err := db.WriteTx()
	if err != nil {
		return err
	}
	defer wtx.Discard()

	err = wtx.Set(databaseKey, val)
	if err != nil {
		return err
	}

	err = wtx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func initializeDB(db kv.DB, name sql.Identifier) (*encoding.DatabaseMetadata, error) {

	md := encoding.DatabaseMetadata{
		Version:     kvrowsVersion,
		Name:        name.String(),
		Opens:       1,
		NextTableID: 1000,
	}

	err := setDatabaseMetadata(db, &md)
	if err != nil {
		return nil, err
	}
	return &md, nil
}

func (e Engine) CreateDatabase(svcs engine.Services, name sql.Identifier, path string,
	options engine.Options) (engine.Database, error) {

	_, err := os.Stat(path)
	if err == nil {
		return nil, fmt.Errorf("kvrows: existing file or directory at %s", path)
	}

	db, err := e.Engine.Open(path)
	if err != nil {
		return nil, err
	}

	md, err := initializeDB(db, name)
	if err != nil {
		os.RemoveAll(path)
		return nil, err
	}
	return &database{
		lockService:   svcs.LockService(),
		metadata:      md,
		name:          name,
		path:          path,
		db:            db,
		tableMetadata: map[sql.Identifier]*encoding.TableMetadata{},
	}, nil
}

func (kvdb *database) Message() string {
	return ""
}

func (kvdb *database) lookupTableMetadata(tblname sql.Identifier) (*encoding.TableMetadata, error) {
	md, ok := kvdb.tableMetadata[tblname]
	if ok {
		return md, nil
	}

	// XXX: need to lookup table metadata from kv

	return nil, fmt.Errorf("kvdb: table %s.%s not found", kvdb.name, tblname)
}

func (kvdb *database) LookupTable(ses engine.Session, tx interface{},
	tblname sql.Identifier) (engine.Table, error) {

	tctx := tx.(*tcontext)
	tbl, ok := tctx.tables[tblname]
	if ok {
		if tbl.dropped && !tbl.created {
			return nil, fmt.Errorf("kvdb: table %s.%s not found", kvdb.name, tblname)
		}
		return tbl, nil
	}

	err := kvdb.lockService.LockTable(ses, tctx.locker, kvdb.name, tblname, fatlock.ACCESS)
	if err != nil {
		return nil, err
	}

	kvdb.mutex.Lock()
	defer kvdb.mutex.Unlock()

	md, err := kvdb.lookupTableMetadata(tblname)
	if err != nil {
		return nil, err
	}

	return kvdb.makeTableContext(tctx, tblname, md)
}

func (kvdb *database) makeTableContext(tctx *tcontext, tblname sql.Identifier,
	md *encoding.TableMetadata) (*table, error) {

	tbl := table{
		db:       kvdb,
		name:     tblname,
		metadata: md,
	}
	for _, col := range md.Columns {
		tbl.columns = append(tbl.columns, sql.QuotedID(col.Name))
		dt, ok := encoding.ToDataType(col.Type)
		if !ok {
			return nil, fmt.Errorf("kvrows: table %s.%s: unexpected encoded column data type: %d",
				kvdb.name, tblname, col.Type)
		}
		tbl.columnTypes = append(tbl.columnTypes, sql.ColumnType{
			Type:    dt,
			Size:    col.Size,
			Fixed:   col.Fixed,
			Binary:  col.Binary,
			NotNull: col.NotNull,
			//Default: col.Default, // XXX: Expr <-> string
		})
	}

	tctx.tables[tblname] = &tbl
	return &tbl, nil
}

func (kvdb *database) newTableID() (uint32, error) {
	id := kvdb.metadata.NextTableID
	kvdb.metadata.NextTableID += 1
	err := setDatabaseMetadata(kvdb.db, kvdb.metadata)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (kvdb *database) CreateTable(ses engine.Session, tx interface{}, tblname sql.Identifier,
	cols []sql.Identifier, colTypes []sql.ColumnType) error {

	tctx := tx.(*tcontext)
	err := kvdb.lockService.LockTable(ses, tctx.locker, kvdb.name, tblname, fatlock.EXCLUSIVE)
	if err != nil {
		return err
	}

	var dropped bool
	if tbl, ok := tctx.tables[tblname]; ok {
		if tbl.dropped && !tbl.created {
			dropped = true
		} else {
			return fmt.Errorf("kvdb: table %s.%s already exists", kvdb.name, tblname)
		}
	}

	kvdb.mutex.Lock()
	defer kvdb.mutex.Unlock()

	if !dropped {
		if _, err := kvdb.lookupTableMetadata(tblname); err == nil {
			return fmt.Errorf("kvdb: table %s.%s already exists", kvdb.name, tblname)
		}
	}

	id, err := kvdb.newTableID()
	if err != nil {
		return err
	}

	md := encoding.TableMetadata{
		ID: id,
	}

	for idx := range cols {
		md.Columns = append(md.Columns, &encoding.ColumnMetadata{
			Name:    cols[idx].String(),
			Index:   uint32(idx),
			Type:    encoding.FromDataType(colTypes[idx].Type),
			Size:    colTypes[idx].Size,
			Fixed:   colTypes[idx].Fixed,
			Binary:  colTypes[idx].Binary,
			NotNull: colTypes[idx].NotNull,
			//Default: colTypes[idx].Default, // XXX: Expr
		})
	}

	tbl, err := kvdb.makeTableContext(tctx, tblname, &md)
	if err != nil {
		return err
	}
	tbl.dropped = dropped
	tbl.created = true
	return nil
}

func (kvdb *database) DropTable(ses engine.Session, tx interface{}, tblname sql.Identifier,
	exists bool) error {

	tctx := tx.(*tcontext)
	err := kvdb.lockService.LockTable(ses, tctx.locker, kvdb.name, tblname, fatlock.EXCLUSIVE)
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
			return fmt.Errorf("kvrows: table %s.%s does not exist", kvdb.name, tblname)
		}
		tbl.dropped = true
		return nil
	}

	kvdb.mutex.Lock()
	defer kvdb.mutex.Unlock()

	_, ok := kvdb.tableMetadata[tblname]
	if !ok {
		if exists {
			return nil
		}
		return fmt.Errorf("kvrows: table %s.%s does not exist", kvdb.name, tblname)
	}

	tctx.tables[tblname] = &table{
		db:      kvdb,
		name:    tblname,
		dropped: true,
	}

	// XXX: also need to delete the persistent state: name, metadata, and rows => at commit time

	return nil
}

func (kvdb *database) ListTables(ses engine.Session, tx interface{}) ([]engine.TableEntry, error) {
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

	kvdb.mutex.Lock()
	defer kvdb.mutex.Unlock()

	for name := range kvdb.tableMetadata {
		if _, ok := tctx.tables[name]; !ok {
			tbls = append(tbls, engine.TableEntry{
				Name: name,
				Type: engine.PhysicalType,
			})
		}
	}
	return tbls, nil
}

func (kvdb *database) Begin(lkr fatlock.Locker) interface{} {
	return &tcontext{
		locker: lkr,
		tables: map[sql.Identifier]*table{},
	}
}

func (kvdb *database) Commit(ses engine.Session, tx interface{}) error {
	tctx := tx.(*tcontext)

	kvdb.mutex.Lock()
	defer kvdb.mutex.Unlock()

	for _, tbl := range tctx.tables {
		if tbl.dropped {
			delete(kvdb.tableMetadata, tbl.name)
		}
		if tbl.created {
			kvdb.tableMetadata[tbl.name] = tbl.metadata
		}
	}

	// XXX: persist changes to the metadata

	return nil
}

func (kvdb *database) Rollback(tctx interface{}) error {
	// XXX: handle rolling back changes to rows
	return nil
}

func (kvdb *database) NextStmt(tctx interface{}) {}

func (kvdb *database) CanClose(drop bool) bool {
	return true
}

func (kvdb *database) Close(drop bool) error {
	// XXX: wait until all transactions are done

	err := kvdb.db.Close()
	if err != nil {
		return err
	}
	if drop {
		return os.RemoveAll(kvdb.path)
	}
	return nil
}

func (kvt *table) Columns(ses engine.Session) []sql.Identifier {
	return kvt.columns
}

func (kvt *table) ColumnTypes(ses engine.Session) []sql.ColumnType {
	return kvt.columnTypes
}

func (kvt *table) Rows(ses engine.Session) (engine.Rows, error) {
	return nil, errors.New("kvrows: Rows: not implemented")
}

func (kvt *table) Insert(ses engine.Session, row []sql.Value) error {
	return errors.New("kvrows: Insert: not implemented")
}

func (kvr *rows) Columns() []sql.Identifier {
	return nil
}

func (kvr *rows) Close() error {
	return errors.New("kvrows: Close: not implemented")
}

func (kvr *rows) Next(ses engine.Session, dest []sql.Value) error {
	return errors.New("kvrows: Next: not implemented")
}

func (kvr *rows) Delete(ses engine.Session) error {
	return errors.New("kvrows: Delete: not implemented")
}

func (kvr *rows) Update(ses engine.Session, updates []sql.ColumnUpdate) error {
	return errors.New("kvrows: Update: not implemented")
}
