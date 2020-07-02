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
	// MIDs 0 to 127 for use by stores.
	sequencesMID   = 128
	databasesMID   = 129
	schemasMID     = 130
	tablesMID      = 131
	indexesMID     = 132
	maxReservedMID = 2048

	midSequence = "mid"
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
	MID      int64
	Metadata []byte
}

type indexRow struct {
	Database string
	Schema   string
	Table    string
	Index    string
}

type PersistentStore interface {
	Table(ctx context.Context, tx sql.Transaction, tn sql.TableName, mid int64,
		tt *engine.TableType) (sql.Table, error)
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
			[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("table"), sql.ID("mid"),
				sql.ID("metadata")},
			[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType, sql.Int64ColType,
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
	tbl, err := st.ps.Table(ctx, tx, sequencesTableName, sequencesMID, st.sequences)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(sequencesTableName, tbl, st.sequences)
	err = ttbl.Insert(ctx,
		sequenceRow{
			Sequence: midSequence,
			Current:  maxReservedMID,
		})
	if err != nil {
		return err
	}

	tbl, err = st.ps.Table(ctx, tx, databasesTableName, databasesMID, st.databases)
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

	tbl, err = st.ps.Table(ctx, tx, schemasTableName, schemasMID, st.schemas)
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
	err = st.createTable(ctx, tx, sequencesTableName, sequencesMID, st.sequences)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = st.createTable(ctx, tx, databasesTableName, databasesMID, st.databases)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = st.createTable(ctx, tx, schemasTableName, schemasMID, st.schemas)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = st.createTable(ctx, tx, tablesTableName, tablesMID, st.tables)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = st.createTable(ctx, tx, indexesTableName, indexesMID, st.indexes)
	if err != nil {
		return err
	}

	return nil
}

func (st *Store) createDatabase(ctx context.Context, tx sql.Transaction,
	dbname sql.Identifier) error {

	tbl, err := st.ps.Table(ctx, tx, databasesTableName, databasesMID, st.databases)
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

	tbl, err := st.ps.Table(ctx, tx, databasesTableName, databasesMID, st.databases)
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

	tbl, err := st.ps.Table(ctx, tx, schemasTableName, schemasMID, st.schemas)
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

	tbl, err := st.ps.Table(ctx, tx, schemasTableName, schemasMID, st.schemas)
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

	tbl, err := st.ps.Table(ctx, tx, schemasTableName, schemasMID, st.schemas)
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

	tbl, err := st.ps.Table(ctx, tx, tablesTableName, tablesMID, st.tables)
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

func (st *Store) LookupTable(ctx context.Context, tx sql.Transaction, tn sql.TableName) (sql.Table,
	*engine.TableType, error) {

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
	mid := tr.MID

	tt, err := engine.DecodeTableType(tn, tr.Metadata)
	if err != nil {
		return nil, nil, err
	}

	tbl, err := st.ps.Table(ctx, tx, tn, mid, tt)
	if err != nil {
		return nil, nil, err
	}
	return tbl, tt, err
}

func (st *Store) createTable(ctx context.Context, tx sql.Transaction, tn sql.TableName, mid int64,
	tt *engine.TableType) error {

	err := st.updateSchema(ctx, tx, tn.SchemaName(), 1)
	if err != nil {
		return err
	}

	md, err := tt.Encode()
	if err != nil {
		return err
	}

	tbl, err := st.ps.Table(ctx, tx, tablesTableName, tablesMID, st.tables)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(tablesTableName, tbl, st.tables)

	err = ttbl.Insert(ctx,
		tableRow{
			Database: tn.Database.String(),
			Schema:   tn.Schema.String(),
			Table:    tn.Table.String(),
			MID:      mid,
			Metadata: md,
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

	mid, err := st.nextSequenceValue(ctx, tx, midSequence)
	if err != nil {
		return err
	}

	return st.createTable(ctx, tx, tn, mid, tt)
}

func (st *Store) DropTable(ctx context.Context, tx sql.Transaction, tn sql.TableName,
	ifExists bool) error {

	err := st.updateSchema(ctx, tx, tn.SchemaName(), -1)
	if err != nil {
		return err
	}

	tbl, err := st.ps.Table(ctx, tx, tablesTableName, tablesMID, st.tables)
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

	tbl, err := st.ps.Table(ctx, tx, indexesTableName, indexesMID, st.indexes)
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

	tbl, err := st.ps.Table(ctx, tx, indexesTableName, indexesMID, st.indexes)
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

	tbl, err := st.ps.Table(ctx, tx, indexesTableName, indexesMID, st.indexes)
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
	tbl, err := st.ps.Table(ctx, tx, databasesTableName, databasesMID, st.databases)
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

	tbl, err := st.ps.Table(ctx, tx, schemasTableName, schemasMID, st.schemas)
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

	tbl, err := st.ps.Table(ctx, tx, tablesTableName, tablesMID, st.tables)
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

	tbl, err := st.ps.Table(ctx, tx, sequencesTableName, sequencesMID, st.sequences)
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
