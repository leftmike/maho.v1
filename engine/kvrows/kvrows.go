package kvrows

/*
- enhanced kv layer that incorporates mvcc:
-- implement simple sql layer in terms of enhanced kv layer
-- allow both layers to be distributed / remote / sharded
-- https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20181209_lazy_txn_record_creation.md
-- need higher level operations on kvrows

- <table-id>/<index-id>/<primary-key>[@<version>]:<row>|<protobuf>

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
- Proposal points to a transaction
- ProposedWrite is a write corresponding to the Proposal

- future: add tableLock and move table access locking there; add a comment for now
- future: move DatabaseMetadata.NextTableID to be a sequence

- tableMetadata needs to be a versioned table: name:metadata
*/

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/fatlock"
	"github.com/leftmike/maho/engine/kv"
	"github.com/leftmike/maho/engine/kvrows/encoding"
	"github.com/leftmike/maho/sql"
)

var (
	primaryIID          = uint32(1)
	databaseMetadataTID = uint32(1)                        // :database-metadata
	tableMetadataTID    = uint32(encoding.MinVersionedTID) // name:table-metadata

	databaseKey   = encoding.MakeKey(databaseMetadataTID, primaryIID, nil)
	kvrowsVersion = uint32(1)
)

type Engine struct {
	Engine kv.Engine
}

type database struct {
	lockService       fatlock.LockService
	mutex             sync.Mutex
	metadata          *encoding.DatabaseMetadata
	name              sql.Identifier
	path              string
	db                kv.DB
	nextTransactionID uint32
	tableMetadata     map[sql.Identifier]*encoding.TableMetadata
}

type tcontext struct {
	locker    fatlock.Locker
	whichOpen uint64
	txid      uint32
	sid       uint32
	txKey     []byte
	tables    map[sql.Identifier]*table
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
			if !encoding.ParseProtobufValue(val, &md) {
				return fmt.Errorf("kvrows: database metadata corrupted in %s", name)
			}
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

	if md.DatabaseVersion != kvrowsVersion {
		return nil, fmt.Errorf("kvrows: %s: unsupported version: %d", path, md.DatabaseVersion)
	}
	if md.Name != name.String() {
		return nil, fmt.Errorf("kvrows: %s: need %s for database name", path, md.Name)
	}

	md.Opens += 1
	err = setValue(db, databaseKey, encoding.MakeProtobufValue(md))
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

func setValue(db kv.DB, key, val []byte) error {
	wtx, err := db.WriteTx()
	if err != nil {
		return err
	}
	defer wtx.Discard()

	err = wtx.Set(key, val)
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
		Type:            uint32(encoding.Type_DatabaseMetadataType),
		DatabaseVersion: kvrowsVersion,
		Name:            name.String(),
		Opens:           1,
		NextTableID:     encoding.MinVersionedTID * 2, // Leave space for versioned system tables.
		NextVersion:     encoding.MinVersion,

		// XXX: replace with primary keys, indexes, and fall back to a sequence
		NextRowID: 1,
	}

	err := setValue(db, databaseKey, encoding.MakeProtobufValue(&md))
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

	err := setValue(kvdb.db, databaseKey, encoding.MakeProtobufValue(kvdb.metadata))
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

	tid, err := kvdb.newTableID()
	if err != nil {
		return err
	}

	md := encoding.TableMetadata{
		Type: uint32(encoding.Type_TableMetadataType),
		ID:   tid,
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

	err = tctx.proposeValue(kvdb.db, tid, primaryIID,
		[]sql.Value{sql.StringValue(tblname.String())}, encoding.MakeProtobufValue(&md))
	if err != nil {
		return err
	}

	tbl := table{
		db:          kvdb,
		name:        tblname,
		metadata:    &md,
		columns:     cols,
		columnTypes: colTypes,
	}
	tbl.dropped = dropped
	tbl.created = true

	tctx.tables[tblname] = &tbl
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
	kvdb.mutex.Lock()
	defer kvdb.mutex.Unlock()

	txid := kvdb.nextTransactionID
	kvdb.nextTransactionID += 1
	return &tcontext{
		locker:    lkr,
		whichOpen: kvdb.metadata.Opens,
		txid:      txid,
		tables:    map[sql.Identifier]*table{},
	}
}

func (kvdb *database) Commit(ses engine.Session, tx interface{}) error {
	kvdb.Dump() // XXX

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

func (kvdb *database) NextStmt(tx interface{}) {
	tctx := tx.(*tcontext)
	tctx.sid += 1
}

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

func (kvdb *database) Dump() error {
	rtx, err := kvdb.db.ReadTx()
	if err != nil {
		return err
	}
	defer rtx.Discard()

	it := rtx.Iterate(nil)
	defer it.Close()

	it.Rewind()
	for it.Valid() {
		it.Value(func(val []byte) error {
			fmt.Printf("%s:%s\n", encoding.FormatKey(it.Key()),
				encoding.FormatValue(val))
			return nil
		})
		it.Next()
	}

	fmt.Println()
	fmt.Println()
	return nil
}

func (tctx *tcontext) proposeValue(db kv.DB, tid, iid uint32, kvals []sql.Value, val []byte) error {
	wtx, err := db.WriteTx()
	if err != nil {
		return err
	}
	defer wtx.Discard()

	// Make sure that a transaction has been started.
	txKey := tctx.txKey
	if txKey == nil {
		txKey = encoding.MakeVersionKey(tid, iid, kvals, encoding.MakeTransactionVersion(tctx.txid))
		td := encoding.Transaction{
			Type:      uint32(encoding.Type_TransactionType),
			State:     uint32(encoding.TransactionState_Active),
			WhichOpen: tctx.whichOpen,
		}
		err = wtx.Set(txKey, encoding.MakeProtobufValue(&td))
		if err != nil {
			return err
		}
	}

	// Might be the first write to this key for this transaction.
	var pd encoding.Proposal
	proposalKey := encoding.MakeVersionKey(tid, iid, kvals, encoding.ProposalVersion)
	err = wtx.Get(proposalKey,
		func(val []byte) error {
			if !encoding.ParseProtobufValue(val, &pd) {
				panic(fmt.Sprintf("kvrows: proposal corrupted for key %s",
					encoding.FormatKey(proposalKey)))
			}
			return nil
		})
	if err == nil {
		if !bytes.Equal(txKey, pd.TransactionKey) {
			return fmt.Errorf("kvrows: conflicting change for key %s",
				encoding.FormatKey(proposalKey))
		}
	} else {
		pd = encoding.Proposal{
			Type:           uint32(encoding.Type_ProposalType),
			TransactionKey: txKey,
		}
		err = wtx.Set(proposalKey, encoding.MakeProtobufValue(&pd))
		if err != nil {
			return err
		}
	}

	// Finally, make the proposed write.
	key := encoding.MakeVersionKey(tid, iid, kvals, encoding.MakeProposedWriteVersion(tctx.sid))
	err = wtx.Set(key, val)
	if err != nil {
		return err
	}

	err = wtx.Commit()
	if err != nil {
		return err
	}

	tctx.txKey = txKey
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
