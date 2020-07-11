package storage

//go:generate protoc --go_opt=paths=source_relative --go_out=. info.proto

import (
	"context"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage/util"
)

const (
	// TIDs 0 to 127 for use by stores.
	sequencesTID   = 128
	databasesTID   = 129
	schemasTID     = 130
	tablesTID      = 131
	indexesTID     = 132
	maxReservedTID = 2048

	tidSequence = "tid"

	PrimaryIID = 1
)

var (
	sequencesTableName = sql.TableName{sql.SYSTEM, sql.PRIVATE, sql.SEQUENCES}
	databasesTableName = sql.TableName{sql.SYSTEM, sql.PRIVATE, sql.DATABASES}
	schemasTableName   = sql.TableName{sql.SYSTEM, sql.PRIVATE, sql.SCHEMAS}
	tablesTableName    = sql.TableName{sql.SYSTEM, sql.PRIVATE, sql.TABLES}
	indexesTableName   = sql.TableName{sql.SYSTEM, sql.PRIVATE, sql.INDEXES}
)

type sequenceRow struct {
	Sequence string
	Current  int64
}

type databaseRow struct {
	Database string
}

type schemaRow struct {
	Database string
	Schema   string
	Tables   int64
}

type tableRow struct {
	Database string
	Schema   string
	Table    string
	TID      int64
	NextIID  int64
	Metadata []byte
	Info     []byte
}

type indexRow struct { // XXX: remove
	Database string
	Schema   string
	Table    string
	Index    string
}

type IndexType struct {
	sql.IndexType
	IID int64
}

type PersistentStore interface {
	Table(ctx context.Context, tx sql.Transaction, tn sql.TableName, tid int64,
		tt *engine.TableType, its []IndexType) (engine.Table, error)
	Begin(sesid uint64) sql.Transaction
}

type Store struct {
	name      string
	ps        PersistentStore
	sequences *engine.TableType
	databases *engine.TableType
	schemas   *engine.TableType
	tables    *engine.TableType
	indexes   *engine.TableType
}

func NewStore(name string, ps PersistentStore, init bool) (*Store, error) {
	st := &Store{
		name: name,
		ps:   ps,
		sequences: engine.MakeTableType(
			[]sql.Identifier{sql.ID("sequence"), sql.ID("current")},
			[]sql.ColumnType{sql.IdColType, sql.Int64ColType},
			[]sql.ColumnKey{sql.MakeColumnKey(0, false)}),

		databases: engine.MakeTableType(
			[]sql.Identifier{sql.ID("database")},
			[]sql.ColumnType{sql.IdColType},
			[]sql.ColumnKey{sql.MakeColumnKey(0, false)}),

		schemas: engine.MakeTableType(
			[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("tables")},
			[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.Int64ColType},
			[]sql.ColumnKey{sql.MakeColumnKey(0, false), sql.MakeColumnKey(1, false)}),

		tables: engine.MakeTableType(
			[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("table"), sql.ID("tid"),
				sql.ID("nextiid"), sql.ID("metadata"), sql.ID("info")},
			[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType, sql.Int64ColType,
				sql.Int64ColType,
				{Type: sql.BytesType, Fixed: false, Size: sql.MaxColumnSize},
				{Type: sql.BytesType, Fixed: false, Size: sql.MaxColumnSize}},
			[]sql.ColumnKey{sql.MakeColumnKey(0, false), sql.MakeColumnKey(1, false),
				sql.MakeColumnKey(2, false)}),

		indexes: engine.MakeTableType(
			[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("table"), sql.ID("index")},
			[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType, sql.IdColType},
			[]sql.ColumnKey{sql.MakeColumnKey(0, false), sql.MakeColumnKey(1, false),
				sql.MakeColumnKey(2, false), sql.MakeColumnKey(3, false)}),
	}
	if init {
		ctx := context.Background()
		tx := st.ps.Begin(0)
		err := st.init(ctx, tx)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}
	}

	return st, nil
}

func (st *Store) init(ctx context.Context, tx sql.Transaction) error {
	tbl, err := st.ps.Table(ctx, tx, sequencesTableName, sequencesTID, st.sequences, nil)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(sequencesTableName, tbl, st.sequences)
	err = ttbl.Insert(ctx,
		sequenceRow{
			Sequence: tidSequence,
			Current:  int64(maxReservedTID),
		})
	if err != nil {
		return err
	}

	tbl, err = st.ps.Table(ctx, tx, databasesTableName, databasesTID, st.databases, nil)
	if err != nil {
		return err
	}
	ttbl = util.MakeTypedTable(databasesTableName, tbl, st.databases)
	err = ttbl.Insert(ctx,
		databaseRow{
			Database: sql.SYSTEM.String(),
		})
	if err != nil {
		return err
	}

	tbl, err = st.ps.Table(ctx, tx, schemasTableName, schemasTID, st.schemas, nil)
	if err != nil {
		return err
	}
	ttbl = util.MakeTypedTable(schemasTableName, tbl, st.schemas)
	err = ttbl.Insert(ctx,
		schemaRow{
			Database: sql.SYSTEM.String(),
			Schema:   sql.PRIVATE.String(),
			Tables:   0,
		})
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = st.createTable(ctx, tx, sequencesTableName, sequencesTID, st.sequences)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = st.createTable(ctx, tx, databasesTableName, databasesTID, st.databases)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = st.createTable(ctx, tx, schemasTableName, schemasTID, st.schemas)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = st.createTable(ctx, tx, tablesTableName, tablesTID, st.tables)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = st.createTable(ctx, tx, indexesTableName, indexesTID, st.indexes)
	if err != nil {
		return err
	}

	return nil
}

func (st *Store) createDatabase(ctx context.Context, tx sql.Transaction,
	dbname sql.Identifier) error {

	tbl, err := st.ps.Table(ctx, tx, databasesTableName, databasesTID, st.databases, nil)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(databasesTableName, tbl, st.databases)

	err = ttbl.Insert(ctx,
		databaseRow{
			Database: dbname.String(),
		})
	if err != nil {
		return err
	}

	tx.NextStmt()
	return st.CreateSchema(ctx, tx, sql.SchemaName{dbname, sql.PUBLIC})
}

func (st *Store) CreateDatabase(dbname sql.Identifier, options map[sql.Identifier]string) error {
	if len(options) != 0 {
		return fmt.Errorf("%s: unexpected option to create database: %s", st.name, dbname)
	}

	ctx := context.Background()
	tx := st.ps.Begin(0)
	err := st.createDatabase(ctx, tx, dbname)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

func (st *Store) lookupDatabase(ctx context.Context, tx sql.Transaction,
	dbname sql.Identifier) (*util.TypedRows, error) {

	tbl, err := st.ps.Table(ctx, tx, databasesTableName, databasesTID, st.databases, nil)
	if err != nil {
		return nil, err
	}
	ttbl := util.MakeTypedTable(databasesTableName, tbl, st.databases)

	keyRow := databaseRow{Database: dbname.String()}
	rows, err := ttbl.Rows(ctx, keyRow, keyRow)
	if err != nil {
		return nil, err
	}

	var dr databaseRow
	err = rows.Next(ctx, &dr)
	if err != nil {
		rows.Close()
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	return rows, nil
}

func (st *Store) validDatabase(ctx context.Context, tx sql.Transaction,
	dbname sql.Identifier) (bool, error) {

	rows, err := st.lookupDatabase(ctx, tx, dbname)
	if err != nil {
		return false, err
	}
	if rows == nil {
		return false, nil
	}
	rows.Close()
	return true, nil
}

func (st *Store) dropDatabase(ctx context.Context, tx sql.Transaction,
	dbname sql.Identifier, ifExists bool) error {

	rows, err := st.lookupDatabase(ctx, tx, dbname)
	if err != nil {
		return err
	}
	if rows == nil {
		if ifExists {
			return nil
		}
		return fmt.Errorf("%s: database %s does not exist", st.name, dbname)
	}
	defer rows.Close()

	scnames, err := st.ListSchemas(ctx, tx, dbname)
	if err != nil {
		return err
	}
	for _, scname := range scnames {
		err = st.DropSchema(ctx, tx, sql.SchemaName{dbname, scname}, true)
		if err != nil {
			return err
		}
	}

	return rows.Delete(ctx)
}

func (st *Store) DropDatabase(dbname sql.Identifier, ifExists bool,
	options map[sql.Identifier]string) error {

	if len(options) != 0 {
		return fmt.Errorf("%s: unexpected option to drop database: %s", st.name, dbname)
	}

	ctx := context.Background()
	tx := st.ps.Begin(0)
	err := st.dropDatabase(ctx, tx, dbname, ifExists)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

func (st *Store) CreateSchema(ctx context.Context, tx sql.Transaction, sn sql.SchemaName) error {
	ok, err := st.validDatabase(ctx, tx, sn.Database)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s: database %s not found", st.name, sn.Database)
	}

	tbl, err := st.ps.Table(ctx, tx, schemasTableName, schemasTID, st.schemas, nil)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(schemasTableName, tbl, st.schemas)

	return ttbl.Insert(ctx,
		schemaRow{
			Database: sn.Database.String(),
			Schema:   sn.Schema.String(),
			Tables:   0,
		})
}

func (st *Store) DropSchema(ctx context.Context, tx sql.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	tbl, err := st.ps.Table(ctx, tx, schemasTableName, schemasTID, st.schemas, nil)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(schemasTableName, tbl, st.schemas)

	keyRow := schemaRow{
		Database: sn.Database.String(),
		Schema:   sn.Schema.String(),
	}
	rows, err := ttbl.Rows(ctx, keyRow, keyRow)
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
		return fmt.Errorf("%s: schema %s not found", st.name, sn)
	} else if err != nil {
		return err
	}
	if sr.Tables > 0 {
		return fmt.Errorf("%s: schema %s is not empty", st.name, sn)
	}
	return rows.Delete(ctx)
}

func (st *Store) updateSchema(ctx context.Context, tx sql.Transaction,
	sn sql.SchemaName, delta int64) error {

	tbl, err := st.ps.Table(ctx, tx, schemasTableName, schemasTID, st.schemas, nil)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(schemasTableName, tbl, st.schemas)

	keyRow := schemaRow{
		Database: sn.Database.String(),
		Schema:   sn.Schema.String(),
	}
	rows, err := ttbl.Rows(ctx, keyRow, keyRow)
	if err != nil {
		return err
	}
	defer rows.Close()

	var sr schemaRow
	err = rows.Next(ctx, &sr)
	if err == io.EOF {
		return fmt.Errorf("%s: schema %s not found", st.name, sn)
	} else if err != nil {
		return err
	}

	return rows.Update(ctx,
		struct {
			Tables int64
		}{sr.Tables + delta})
}

func (st *Store) lookupTable(ctx context.Context, tx sql.Transaction,
	tn sql.TableName) (*util.TypedRows, error) {

	tbl, err := st.ps.Table(ctx, tx, tablesTableName, tablesTID, st.tables, nil)
	if err != nil {
		return nil, err
	}
	ttbl := util.MakeTypedTable(tablesTableName, tbl, st.tables)

	keyRow := tableRow{
		Database: tn.Database.String(),
		Schema:   tn.Schema.String(),
		Table:    tn.Table.String(),
	}
	rows, err := ttbl.Rows(ctx, keyRow, keyRow)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (st *Store) validTable(ctx context.Context, tx sql.Transaction, tn sql.TableName) (bool,
	error) {

	rows, err := st.lookupTable(ctx, tx, tn)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var tr tableRow
	err = rows.Next(ctx, &tr)
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func encodeTableInfo(tt *engine.TableType) ([]byte, error) {
	var ti TableInfo
	ti.Indexes = make([]*IndexInfo, 0, len(tt.Indexes()))
	for iid, it := range tt.Indexes() {
		ti.Indexes = append(ti.Indexes,
			&IndexInfo{
				Name: it.Name.String(),
				IID:  int64(iid + 1),
			})
	}

	return proto.Marshal(&ti)
}

func findIndex(tt *engine.TableType, nam sql.Identifier) (sql.IndexType, bool) {
	for _, it := range tt.Indexes() {
		if it.Name == nam {
			return it, true
		}
	}
	return sql.IndexType{}, false
}

func decodeTableInfo(tn sql.TableName, tt *engine.TableType, buf []byte) ([]IndexType, error) {
	var ti TableInfo
	err := proto.Unmarshal(buf, &ti)
	if err != nil {
		return nil, fmt.Errorf("storage: table %s: %s", tn, err)
	}

	its := make([]IndexType, 0, len(ti.Indexes))
	for _, it := range ti.Indexes {
		sit, ok := findIndex(tt, sql.QuotedID(it.Name))
		if !ok {
			return nil, fmt.Errorf("storage: table %s: internal error: index %s not found",
				tn, it.Name)
		}
		its = append(its,
			IndexType{
				IndexType: sit,
				IID:       it.IID,
			})
	}

	return its, nil
}

func (st *Store) LookupTable(ctx context.Context, tx sql.Transaction,
	tn sql.TableName) (engine.Table, *engine.TableType, error) {

	rows, err := st.lookupTable(ctx, tx, tn)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var tr tableRow
	err = rows.Next(ctx, &tr)
	if err != nil {
		if err == io.EOF {
			return nil, nil, fmt.Errorf("%s: table %s not found", st.name, tn)
		}
		return nil, nil, err
	}
	tid := tr.TID

	tt, err := engine.DecodeTableType(tn, tr.Metadata)
	if err != nil {
		return nil, nil, err
	}
	its, err := decodeTableInfo(tn, tt, tr.Info)
	if err != nil {
		return nil, nil, err
	}

	tbl, err := st.ps.Table(ctx, tx, tn, tid, tt, its)
	if err != nil {
		return nil, nil, err
	}
	return tbl, tt, err
}

func (st *Store) createTable(ctx context.Context, tx sql.Transaction, tn sql.TableName, tid int64,
	tt *engine.TableType) error {

	err := st.updateSchema(ctx, tx, tn.SchemaName(), 1)
	if err != nil {
		return err
	}

	md, err := tt.Encode()
	if err != nil {
		return err
	}

	ti, err := encodeTableInfo(tt)
	if err != nil {
		return err
	}

	tbl, err := st.ps.Table(ctx, tx, tablesTableName, tablesTID, st.tables, nil)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(tablesTableName, tbl, st.tables)

	err = ttbl.Insert(ctx,
		tableRow{
			Database: tn.Database.String(),
			Schema:   tn.Schema.String(),
			Table:    tn.Table.String(),
			TID:      tid,
			NextIID:  int64(len(tt.Indexes()) + 1),
			Metadata: md,
			Info:     ti,
		})
	if err != nil {
		return err
	}

	return nil
}

func (st *Store) CreateTable(ctx context.Context, tx sql.Transaction, tn sql.TableName,
	tt *engine.TableType, ifNotExists bool) error {

	ok, err := st.validTable(ctx, tx, tn)
	if err != nil {
		return err
	}
	if ok {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("%s: table %s already exists", st.name, tn)
	}

	tid, err := st.nextSequenceValue(ctx, tx, tidSequence)
	if err != nil {
		return err
	}

	return st.createTable(ctx, tx, tn, tid, tt)
}

func (st *Store) DropTable(ctx context.Context, tx sql.Transaction, tn sql.TableName,
	ifExists bool) error {

	err := st.updateSchema(ctx, tx, tn.SchemaName(), -1)
	if err != nil {
		return err
	}

	tbl, err := st.ps.Table(ctx, tx, tablesTableName, tablesTID, st.tables, nil)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(tablesTableName, tbl, st.tables)

	keyRow := tableRow{
		Database: tn.Database.String(),
		Schema:   tn.Schema.String(),
		Table:    tn.Table.String(),
	}
	rows, err := ttbl.Rows(ctx, keyRow, keyRow)
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
		return fmt.Errorf("%s: table %s not found", st.name, tn)
	} else if err != nil {
		return err
	}

	return rows.Delete(ctx)
}

func (st *Store) lookupIndex(ctx context.Context, tx sql.Transaction, tn sql.TableName,
	idxname sql.Identifier) (bool, error) {

	tbl, err := st.ps.Table(ctx, tx, indexesTableName, indexesTID, st.indexes, nil)
	if err != nil {
		return false, err
	}
	ttbl := util.MakeTypedTable(indexesTableName, tbl, st.indexes)

	keyRow := indexRow{
		Database: tn.Database.String(),
		Schema:   tn.Schema.String(),
		Table:    tn.Table.String(),
		Index:    idxname.String(),
	}
	rows, err := ttbl.Rows(ctx, keyRow, keyRow)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var ir indexRow
	err = rows.Next(ctx, &ir)
	if err == io.EOF {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

func (st *Store) CreateIndex(ctx context.Context, tx sql.Transaction, idxname sql.Identifier,
	tn sql.TableName, unique bool, keys []sql.ColumnKey, ifNotExists bool) error {

	ok, err := st.lookupIndex(ctx, tx, tn, idxname)
	if err != nil {
		return err
	}
	if ok {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("%s: index %s on table %s already exists", st.name, idxname, tn)
	}

	ok, err = st.validTable(ctx, tx, tn)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s: table %s not found", st.name, tn)
	}

	tbl, err := st.ps.Table(ctx, tx, indexesTableName, indexesTID, st.indexes, nil)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(indexesTableName, tbl, st.indexes)

	return ttbl.Insert(ctx,
		indexRow{
			Database: tn.Database.String(),
			Schema:   tn.Schema.String(),
			Table:    tn.Table.String(),
			Index:    idxname.String(),
		})
}

func (st *Store) DropIndex(ctx context.Context, tx sql.Transaction, idxname sql.Identifier,
	tn sql.TableName, ifExists bool) error {

	tbl, err := st.ps.Table(ctx, tx, indexesTableName, indexesTID, st.indexes, nil)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(indexesTableName, tbl, st.indexes)

	keyRow := indexRow{
		Database: tn.Database.String(),
		Schema:   tn.Schema.String(),
		Table:    tn.Table.String(),
		Index:    idxname.String(),
	}
	rows, err := ttbl.Rows(ctx, keyRow, keyRow)
	if err != nil {
		return err
	}
	defer rows.Close()

	var ir indexRow
	err = rows.Next(ctx, &ir)
	if err == io.EOF {
		if ifExists {
			return nil
		}
		return fmt.Errorf("%s: index %s on table %s not found", st.name, idxname, tn)
	} else if err != nil {
		return err
	}

	return rows.Delete(ctx)
}

func (st *Store) Begin(sesid uint64) sql.Transaction {
	return st.ps.Begin(sesid)
}

func (st *Store) ListDatabases(ctx context.Context, tx sql.Transaction) ([]sql.Identifier, error) {
	tbl, err := st.ps.Table(ctx, tx, databasesTableName, databasesTID, st.databases, nil)
	if err != nil {
		return nil, err
	}
	ttbl := util.MakeTypedTable(databasesTableName, tbl, st.databases)

	rows, err := ttbl.Rows(ctx, nil, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dbnames []sql.Identifier
	for {
		var dr databaseRow
		err = rows.Next(ctx, &dr)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		dbnames = append(dbnames, sql.ID(dr.Database))
	}
	return dbnames, nil
}

func (st *Store) ListSchemas(ctx context.Context, tx sql.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	tbl, err := st.ps.Table(ctx, tx, schemasTableName, schemasTID, st.schemas, nil)
	if err != nil {
		return nil, err
	}
	ttbl := util.MakeTypedTable(schemasTableName, tbl, st.schemas)

	rows, err := ttbl.Rows(ctx, schemaRow{Database: dbname.String()}, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

func (st *Store) ListTables(ctx context.Context, tx sql.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	tbl, err := st.ps.Table(ctx, tx, tablesTableName, tablesTID, st.tables, nil)
	if err != nil {
		return nil, err
	}
	ttbl := util.MakeTypedTable(tablesTableName, tbl, st.tables)

	rows, err := ttbl.Rows(ctx,
		tableRow{Database: sn.Database.String(), Table: sn.Schema.String()}, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

func (st *Store) nextSequenceValue(ctx context.Context, tx sql.Transaction,
	sequence string) (int64, error) {

	tbl, err := st.ps.Table(ctx, tx, sequencesTableName, sequencesTID, st.sequences, nil)
	if err != nil {
		return 0, err
	}
	ttbl := util.MakeTypedTable(sequencesTableName, tbl, st.sequences)

	keyRow := sequenceRow{Sequence: sequence}
	rows, err := ttbl.Rows(ctx, keyRow, keyRow)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var sr sequenceRow
	err = rows.Next(ctx, &sr)
	if err != nil {
		return 0, fmt.Errorf("%s: sequence %s not found", st.name, sequence)
	}
	err = rows.Update(ctx,
		struct {
			Current int64
		}{sr.Current + 1})
	if err != nil {
		return 0, err
	}
	return sr.Current + 1, nil
}
