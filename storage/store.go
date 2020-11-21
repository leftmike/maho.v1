package storage

import (
	"context"
	"fmt"
	"io"

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
	maxReservedTID = 2048

	tidSequence = "tid"

	PrimaryIID = 1
)

var (
	sequencesTableName = sql.TableName{sql.SYSTEM, sql.PRIVATE, sql.SEQUENCES}
	databasesTableName = sql.TableName{sql.SYSTEM, sql.PRIVATE, sql.DATABASES}
	schemasTableName   = sql.TableName{sql.SYSTEM, sql.PRIVATE, sql.SCHEMAS}
	tablesTableName    = sql.TableName{sql.SYSTEM, sql.PRIVATE, sql.TABLES}
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
	Database       string
	Schema         string
	Table          string
	TID            int64
	TypeMetadata   []byte
	TypeVersion    int64
	LayoutMetadata []byte
}

type PersistentStore interface {
	Table(ctx context.Context, tx engine.Transaction, tn sql.TableName, tid int64,
		tt *engine.TableType, tl *TableLayout) (engine.Table, error)
	Begin(sesid uint64) engine.Transaction
}

type Store struct {
	name      string
	ps        PersistentStore
	sequences *engine.TableType
	databases *engine.TableType
	schemas   *engine.TableType
	tables    *engine.TableType
}

func NewStore(name string, ps PersistentStore, init bool) (*Store, error) {
	st := &Store{
		name: name,
		ps:   ps,
		sequences: engine.MakeTableType(
			[]sql.Identifier{sql.ID("sequence"), sql.ID("current")},
			[]sql.ColumnType{sql.IdColType, sql.Int64ColType},
			make([]sql.ColumnDefault, 2),
			[]sql.ColumnKey{sql.MakeColumnKey(0, false)}),

		databases: engine.MakeTableType(
			[]sql.Identifier{sql.ID("database")},
			[]sql.ColumnType{sql.IdColType},
			make([]sql.ColumnDefault, 1),
			[]sql.ColumnKey{sql.MakeColumnKey(0, false)}),

		schemas: engine.MakeTableType(
			[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("tables")},
			[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.Int64ColType},
			make([]sql.ColumnDefault, 3),
			[]sql.ColumnKey{sql.MakeColumnKey(0, false), sql.MakeColumnKey(1, false)}),

		tables: engine.MakeTableType(
			[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("table"), sql.ID("tid"),
				sql.ID("typemetadata"), sql.ID("typeversion"), sql.ID("layoutmetadata")},
			[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType, sql.Int64ColType,
				{Type: sql.BytesType, Fixed: false, Size: sql.MaxColumnSize}, sql.Int64ColType,
				{Type: sql.BytesType, Fixed: false, Size: sql.MaxColumnSize}},
			make([]sql.ColumnDefault, 7),
			[]sql.ColumnKey{sql.MakeColumnKey(0, false), sql.MakeColumnKey(1, false),
				sql.MakeColumnKey(2, false)}),
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

func (st *Store) init(ctx context.Context, tx engine.Transaction) error {
	tbl, err := st.ps.Table(ctx, tx, sequencesTableName, sequencesTID, st.sequences,
		makeTableLayout(st.sequences))
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(sequencesTableName, tbl, st.sequences)
	err = ttbl.Insert(ctx,
		sequenceRow{
			Sequence: tidSequence,
			Current:  maxReservedTID,
		})
	if err != nil {
		return err
	}

	tbl, err = st.ps.Table(ctx, tx, databasesTableName, databasesTID, st.databases,
		makeTableLayout(st.databases))
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

	tbl, err = st.ps.Table(ctx, tx, schemasTableName, schemasTID, st.schemas,
		makeTableLayout(st.schemas))
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

	return nil
}

func (st *Store) createDatabase(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) error {

	tbl, err := st.ps.Table(ctx, tx, databasesTableName, databasesTID, st.databases,
		makeTableLayout(st.databases))
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

func (st *Store) lookupDatabase(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) (*util.TypedRows, error) {

	tbl, err := st.ps.Table(ctx, tx, databasesTableName, databasesTID, st.databases,
		makeTableLayout(st.databases))
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

func (st *Store) validDatabase(ctx context.Context, tx engine.Transaction,
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

func (st *Store) dropDatabase(ctx context.Context, tx engine.Transaction,
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

func (st *Store) CreateSchema(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) error {

	ok, err := st.validDatabase(ctx, tx, sn.Database)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s: database %s not found", st.name, sn.Database)
	}

	tbl, err := st.ps.Table(ctx, tx, schemasTableName, schemasTID, st.schemas,
		makeTableLayout(st.schemas))
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

func (st *Store) DropSchema(ctx context.Context, tx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	tbl, err := st.ps.Table(ctx, tx, schemasTableName, schemasTID, st.schemas,
		makeTableLayout(st.schemas))
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

func (st *Store) updateSchema(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName, delta int64) error {

	tbl, err := st.ps.Table(ctx, tx, schemasTableName, schemasTID, st.schemas,
		makeTableLayout(st.schemas))
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

func (st *Store) lookupTableRows(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (*util.TypedRows, error) {

	tbl, err := st.ps.Table(ctx, tx, tablesTableName, tablesTID, st.tables,
		makeTableLayout(st.tables))
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

func (st *Store) validTable(ctx context.Context, tx engine.Transaction, tn sql.TableName) (bool,
	error) {

	rows, err := st.lookupTableRows(ctx, tx, tn)
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

func (st *Store) lookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName, wantTbl bool) (engine.Table, *engine.TableType, error) {

	rows, err := st.lookupTableRows(ctx, tx, tn)
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

	tt, err := engine.DecodeTableType(tn, tr.TypeMetadata)
	if err != nil {
		return nil, nil, err
	}
	if tt.Version() != tr.TypeVersion {
		return nil, nil, fmt.Errorf("%s: table %s metadata corrupted", st.name, tn)
	}
	if !wantTbl {
		return nil, tt, nil
	}

	tl, err := st.decodeTableLayout(tn, tt, tr.LayoutMetadata)
	if err != nil {
		return nil, nil, err
	}

	tbl, err := st.ps.Table(ctx, tx, tn, tid, tt, tl)
	if err != nil {
		return nil, nil, err
	}
	return tbl, tt, nil
}

func (st *Store) LookupTableType(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (*engine.TableType, error) {

	_, tt, err := st.lookupTable(ctx, tx, tn, false)
	return tt, err
}

func (st *Store) LookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, *engine.TableType, error) {

	return st.lookupTable(ctx, tx, tn, true)
}

func (st *Store) createTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	tid int64, tt *engine.TableType) error {

	err := st.updateSchema(ctx, tx, tn.SchemaName(), 1)
	if err != nil {
		return err
	}

	typmd, err := tt.Encode()
	if err != nil {
		return err
	}

	tl := makeTableLayout(tt)
	lyomd, err := tl.encode()
	if err != nil {
		return err
	}

	tbl, err := st.ps.Table(ctx, tx, tablesTableName, tablesTID, st.tables,
		makeTableLayout(st.tables))
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(tablesTableName, tbl, st.tables)

	err = ttbl.Insert(ctx,
		tableRow{
			Database:       tn.Database.String(),
			Schema:         tn.Schema.String(),
			Table:          tn.Table.String(),
			TID:            tid,
			TypeMetadata:   typmd,
			TypeVersion:    tt.Version(),
			LayoutMetadata: lyomd,
		})
	if err != nil {
		return err
	}

	return nil
}

func (st *Store) CreateTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
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

func (st *Store) DropTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	err := st.updateSchema(ctx, tx, tn.SchemaName(), -1)
	if err != nil {
		return err
	}

	rows, err := st.lookupTableRows(ctx, tx, tn)
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

func (st *Store) UpdateType(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	tt *engine.TableType) error {

	return st.updateLayout(ctx, tx, tn, tt, nil)
}

func (st *Store) updateLayout(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	tt *engine.TableType, update func(tl *TableLayout) error) error {

	rows, err := st.lookupTableRows(ctx, tx, tn)
	if err != nil {
		return err
	}
	defer rows.Close()

	var tr tableRow
	err = rows.Next(ctx, &tr)
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("%s: table %s not found", st.name, tn)
		}
		return err
	}

	ptt, err := engine.DecodeTableType(tn, tr.TypeMetadata)
	if err != nil {
		return err
	}
	if ptt.Version() != tr.TypeVersion {
		return fmt.Errorf("%s: table %s metadata corrupted", st.name, tn)
	}

	tl, err := st.decodeTableLayout(tn, ptt, tr.LayoutMetadata)
	if err != nil {
		return err
	}

	if ptt.Version() != tt.Version()-1 {
		return fmt.Errorf("%s: table %s: conflicting metadata update", st.name, tn)
	}

	if update != nil {
		err = update(tl)
		if err != nil {
			return err
		}
	}

	typmd, err := tt.Encode()
	if err != nil {
		return err
	}

	lyomd, err := tl.encode()
	if err != nil {
		return err
	}

	return rows.Update(ctx,
		struct {
			TypeMetadata   []byte
			TypeVersion    int64
			LayoutMetadata []byte
		}{typmd, tt.Version(), lyomd})
}

func (st *Store) AddIndex(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	tt *engine.TableType, it sql.IndexType) error {

	return st.updateLayout(ctx, tx, tn, tt,
		func(tl *TableLayout) error {
			tl.addIndexLayout(it)
			return nil
		})
}

func (st *Store) RemoveIndex(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	tt *engine.TableType, rdx int) error {

	return st.updateLayout(ctx, tx, tn, tt,
		func(tl *TableLayout) error {
			indexes := make([]IndexLayout, 0, len(tl.indexes)-1)
			for idx, il := range tl.indexes {
				if idx == rdx {
					continue
				}
				indexes = append(indexes, il)
			}
			tl.indexes = indexes

			return nil
		})
}

func (st *Store) Begin(sesid uint64) engine.Transaction {
	return st.ps.Begin(sesid)
}

func (st *Store) ListDatabases(ctx context.Context, tx engine.Transaction) ([]sql.Identifier,
	error) {

	tbl, err := st.ps.Table(ctx, tx, databasesTableName, databasesTID, st.databases,
		makeTableLayout(st.databases))
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

func (st *Store) ListSchemas(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	tbl, err := st.ps.Table(ctx, tx, schemasTableName, schemasTID, st.schemas,
		makeTableLayout(st.schemas))
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

func (st *Store) ListTables(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	tbl, err := st.ps.Table(ctx, tx, tablesTableName, tablesTID, st.tables,
		makeTableLayout(st.tables))
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

func (st *Store) nextSequenceValue(ctx context.Context, tx engine.Transaction,
	sequence string) (int64, error) {

	tbl, err := st.ps.Table(ctx, tx, sequencesTableName, sequencesTID, st.sequences,
		makeTableLayout(st.sequences))
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
