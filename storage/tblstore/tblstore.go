package tblstore

//go:generate protoc --go_opt=paths=source_relative --go_out=. metadata.proto

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
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

type TableStruct interface {
	Table(ctx context.Context, tx storage.Transaction) (storage.Table, error)
	// XXX: these are not needed
	Columns() []sql.Identifier
	ColumnTypes() []sql.ColumnType
	PrimaryKey() []sql.ColumnKey
}

type store interface {
	NeedsInit() bool
	//MakeTableStruct(tn sql.TableName, mid int64, td TableDef) (TableStruct, error)
	MakeTableStruct(tn sql.TableName, mid int64, cols []sql.Identifier,
		colTypes []sql.ColumnType, primary []sql.ColumnKey) (TableStruct, error)
	Begin(sesid uint64) storage.Transaction
}

type tableStore struct {
	name      string
	st        store
	sequences TableStruct
	databases TableStruct
	schemas   TableStruct
	tables    TableStruct
	indexes   TableStruct
}

func NewStore(name string, st store) (storage.Store, error) {
	sequences, err := st.MakeTableStruct(sequencesTableName, sequencesMID,
		[]sql.Identifier{sql.ID("sequence"), sql.ID("current")},
		[]sql.ColumnType{sql.IdColType, sql.Int64ColType},
		[]sql.ColumnKey{sql.MakeColumnKey(0, false)})
	if err != nil {
		return nil, err
	}
	databases, err := st.MakeTableStruct(databasesTableName, databasesMID,
		[]sql.Identifier{sql.ID("database")},
		[]sql.ColumnType{sql.IdColType},
		[]sql.ColumnKey{sql.MakeColumnKey(0, false)})
	if err != nil {
		return nil, err
	}
	schemas, err := st.MakeTableStruct(schemasTableName, schemasMID,
		[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("tables")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.Int64ColType},
		[]sql.ColumnKey{sql.MakeColumnKey(0, false), sql.MakeColumnKey(1, false)})
	if err != nil {
		return nil, err
	}
	tables, err := st.MakeTableStruct(tablesTableName, tablesMID,
		[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("table"), sql.ID("mid"),
			sql.ID("metadata")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType, sql.Int64ColType,
			{Type: sql.BytesType, Fixed: false, Size: sql.MaxColumnSize}},
		[]sql.ColumnKey{sql.MakeColumnKey(0, false), sql.MakeColumnKey(1, false),
			sql.MakeColumnKey(2, false)})
	if err != nil {
		return nil, err
	}
	indexes, err := st.MakeTableStruct(indexesTableName, indexesMID,
		[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("table"), sql.ID("index")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType, sql.IdColType},
		[]sql.ColumnKey{sql.MakeColumnKey(0, false), sql.MakeColumnKey(1, false),
			sql.MakeColumnKey(2, false), sql.MakeColumnKey(3, false)})
	if err != nil {
		return nil, err
	}

	tblst := &tableStore{
		name:      name,
		st:        st,
		sequences: sequences,
		databases: databases,
		schemas:   schemas,
		tables:    tables,
		indexes:   indexes,
	}
	if st.NeedsInit() {
		ctx := context.Background()
		tx := tblst.st.Begin(0)
		err = tblst.init(ctx, tx)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}
	}

	return tblst, nil
}

func (tblst *tableStore) init(ctx context.Context, tx storage.Transaction) error {
	tbl, err := tblst.sequences.Table(ctx, tx)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(sequencesTableName, tbl)
	err = ttbl.Insert(ctx,
		sequenceRow{
			Sequence: midSequence,
			Current:  maxReservedMID,
		})
	if err != nil {
		return err
	}

	tbl, err = tblst.databases.Table(ctx, tx)
	if err != nil {
		return err
	}
	ttbl = util.MakeTypedTable(databasesTableName, tbl)
	err = ttbl.Insert(ctx,
		databaseRow{
			Database: sql.SYSTEM.String(),
		})
	if err != nil {
		return err
	}

	tbl, err = tblst.schemas.Table(ctx, tx)
	if err != nil {
		return err
	}
	ttbl = util.MakeTypedTable(schemasTableName, tbl)
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
	err = tblst.createTable(ctx, tx, sequencesTableName, sequencesMID, tblst.sequences)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = tblst.createTable(ctx, tx, databasesTableName, databasesMID, tblst.databases)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = tblst.createTable(ctx, tx, schemasTableName, schemasMID, tblst.schemas)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = tblst.createTable(ctx, tx, tablesTableName, tablesMID, tblst.tables)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = tblst.createTable(ctx, tx, indexesTableName, indexesMID, tblst.indexes)
	if err != nil {
		return err
	}

	return nil
}

func (tblst *tableStore) createDatabase(ctx context.Context, tx storage.Transaction,
	dbname sql.Identifier) error {

	tbl, err := tblst.databases.Table(ctx, tx)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(databasesTableName, tbl)

	err = ttbl.Insert(ctx,
		databaseRow{
			Database: dbname.String(),
		})
	if err != nil {
		return err
	}

	tx.NextStmt()
	return tblst.CreateSchema(ctx, tx, sql.SchemaName{dbname, sql.PUBLIC})
}

func (tblst *tableStore) CreateDatabase(dbname sql.Identifier,
	options map[sql.Identifier]string) error {

	if len(options) != 0 {
		return fmt.Errorf("%s: unexpected option to create database: %s", tblst.name, dbname)
	}

	ctx := context.Background()
	tx := tblst.st.Begin(0)
	err := tblst.createDatabase(ctx, tx, dbname)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

func (tblst *tableStore) lookupDatabase(ctx context.Context, tx storage.Transaction,
	dbname sql.Identifier) (*util.Rows, error) {

	tbl, err := tblst.databases.Table(ctx, tx)
	if err != nil {
		return nil, err
	}
	ttbl := util.MakeTypedTable(databasesTableName, tbl)

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

func (tblst *tableStore) validDatabase(ctx context.Context, tx storage.Transaction,
	dbname sql.Identifier) (bool, error) {

	rows, err := tblst.lookupDatabase(ctx, tx, dbname)
	if err != nil {
		return false, err
	}
	if rows == nil {
		return false, nil
	}
	rows.Close()
	return true, nil
}

func (tblst *tableStore) dropDatabase(ctx context.Context, tx storage.Transaction,
	dbname sql.Identifier, ifExists bool) error {

	rows, err := tblst.lookupDatabase(ctx, tx, dbname)
	if err != nil {
		return err
	}
	if rows == nil {
		if ifExists {
			return nil
		}
		return fmt.Errorf("%s: database %s does not exist", tblst.name, dbname)
	}
	defer rows.Close()

	scnames, err := tblst.ListSchemas(ctx, tx, dbname)
	if err != nil {
		return err
	}
	for _, scname := range scnames {
		err = tblst.DropSchema(ctx, tx, sql.SchemaName{dbname, scname}, true)
		if err != nil {
			return err
		}
	}

	return rows.Delete(ctx)
}

func (tblst *tableStore) DropDatabase(dbname sql.Identifier, ifExists bool,
	options map[sql.Identifier]string) error {

	if len(options) != 0 {
		return fmt.Errorf("%s: unexpected option to drop database: %s", tblst.name, dbname)
	}

	ctx := context.Background()
	tx := tblst.st.Begin(0)
	err := tblst.dropDatabase(ctx, tx, dbname, ifExists)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

func (tblst *tableStore) CreateSchema(ctx context.Context, tx storage.Transaction,
	sn sql.SchemaName) error {

	ok, err := tblst.validDatabase(ctx, tx, sn.Database)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s: database %s not found", tblst.name, sn.Database)
	}

	tbl, err := tblst.schemas.Table(ctx, tx)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(schemasTableName, tbl)

	return ttbl.Insert(ctx,
		schemaRow{
			Database: sn.Database.String(),
			Schema:   sn.Schema.String(),
			Tables:   0,
		})
}

func (tblst *tableStore) DropSchema(ctx context.Context, tx storage.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	tbl, err := tblst.schemas.Table(ctx, tx)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(schemasTableName, tbl)

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
		return fmt.Errorf("%s: schema %s not found", tblst.name, sn)
	} else if err != nil {
		return err
	}
	if sr.Tables > 0 {
		return fmt.Errorf("%s: schema %s is not empty", tblst.name, sn)
	}
	return rows.Delete(ctx)
}

func (tblst *tableStore) updateSchema(ctx context.Context, tx storage.Transaction,
	sn sql.SchemaName, delta int64) error {

	tbl, err := tblst.schemas.Table(ctx, tx)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(schemasTableName, tbl)

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
		return fmt.Errorf("%s: schema %s not found", tblst.name, sn)
	} else if err != nil {
		return err
	}

	return rows.Update(ctx,
		struct {
			Tables int64
		}{sr.Tables + delta})
}

func (tblst *tableStore) lookupTable(ctx context.Context, tx storage.Transaction,
	tn sql.TableName) (*util.Rows, error) {

	tbl, err := tblst.tables.Table(ctx, tx)
	if err != nil {
		return nil, err
	}
	ttbl := util.MakeTypedTable(tablesTableName, tbl)

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

func (tblst *tableStore) validTable(ctx context.Context, tx storage.Transaction,
	tn sql.TableName) (bool, error) {

	rows, err := tblst.lookupTable(ctx, tx, tn)
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

func (tblst *tableStore) decodeTableMetadata(tn sql.TableName, mid int64,
	buf []byte) (TableStruct, error) {

	var md TableMetadata
	err := proto.Unmarshal(buf, &md)
	if err != nil {
		return nil, err
	}

	cols := make([]sql.Identifier, 0, len(md.Columns))
	colTypes := make([]sql.ColumnType, 0, len(md.Columns))
	for cdx := range md.Columns {
		cols = append(cols, sql.QuotedID(md.Columns[cdx].Name))

		var dflt sql.Expr
		if md.Columns[cdx].Default != "" {
			p := parser.NewParser(strings.NewReader(md.Columns[cdx].Default),
				fmt.Sprintf("%s metadata", tn))
			dflt, err = p.ParseExpr()
			if err != nil {
				return nil, err
			}
		}
		colTypes = append(colTypes,
			sql.ColumnType{
				Type:    sql.DataType(md.Columns[cdx].Type),
				Size:    md.Columns[cdx].Size,
				Fixed:   md.Columns[cdx].Fixed,
				NotNull: md.Columns[cdx].NotNull,
				Default: dflt,
			})
	}

	primary := make([]sql.ColumnKey, 0, len(md.Primary))
	for _, pk := range md.Primary {
		primary = append(primary, sql.MakeColumnKey(int(pk.Number), pk.Reverse))
	}

	return tblst.st.MakeTableStruct(tn, mid, cols, colTypes, primary)
}

func (tblst *tableStore) LookupTable(ctx context.Context, tx storage.Transaction,
	tn sql.TableName) (storage.Table, error) {

	rows, err := tblst.lookupTable(ctx, tx, tn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tr tableRow
	err = rows.Next(ctx, &tr)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("%s: table %s not found", tblst.name, tn)
		}
		return nil, err
	}
	mid := tr.MID

	ts, err := tblst.decodeTableMetadata(tn, mid, tr.Metadata)
	if err != nil {
		return nil, err
	}

	return ts.Table(ctx, tx)
}

func encodeTableMetadata(ts TableStruct) ([]byte, error) {
	cols := ts.Columns()
	colTypes := ts.ColumnTypes()

	var md TableMetadata
	md.Columns = make([]*ColumnMetadata, 0, len(cols))
	for cdx := range cols {
		var dflt string
		if colTypes[cdx].Default != nil {
			dflt = colTypes[cdx].Default.String()
		}
		md.Columns = append(md.Columns,
			&ColumnMetadata{
				Name:    cols[cdx].String(),
				Type:    DataType(colTypes[cdx].Type),
				Size:    colTypes[cdx].Size,
				Fixed:   colTypes[cdx].Fixed,
				NotNull: colTypes[cdx].NotNull,
				Default: dflt,
			})
	}

	primary := ts.PrimaryKey()
	md.Primary = make([]*ColumnKey, 0, len(primary))
	for _, pk := range primary {
		md.Primary = append(md.Primary,
			&ColumnKey{
				Number:  int32(pk.Number()),
				Reverse: pk.Reverse(),
			})
	}

	return proto.Marshal(&md)
}

func (tblst *tableStore) createTable(ctx context.Context, tx storage.Transaction, tn sql.TableName,
	mid int64, ts TableStruct) error {

	err := tblst.updateSchema(ctx, tx, tn.SchemaName(), 1)
	if err != nil {
		return err
	}

	md, err := encodeTableMetadata(ts)
	if err != nil {
		return err
	}

	tbl, err := tblst.tables.Table(ctx, tx)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(tablesTableName, tbl)

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

func (tblst *tableStore) CreateTable(ctx context.Context, tx storage.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []sql.ColumnKey,
	ifNotExists bool) error {

	if len(primary) == 0 {
		rowID := sql.ID("rowid")

		for _, col := range cols {
			if col == rowID {
				return fmt.Errorf("%s: unable to add %s column for table %s missing primary key",
					tblst.name, rowID, tn)
			}
		}

		primary = []sql.ColumnKey{
			sql.MakeColumnKey(len(cols), false),
		}
		cols = append(cols, rowID)
		colTypes = append(colTypes, sql.ColumnType{
			Type:    sql.IntegerType,
			Size:    8,
			NotNull: true,
			Default: &expr.Call{Name: sql.ID("unique_rowid")},
		})
	}

	ok, err := tblst.validTable(ctx, tx, tn)
	if err != nil {
		return err
	}
	if ok {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("%s: table %s already exists", tblst.name, tn)
	}

	mid, err := tblst.nextSequenceValue(ctx, tx, midSequence)
	if err != nil {
		return err
	}

	ts, err := tblst.st.MakeTableStruct(tn, mid, cols, colTypes, primary)
	if err != nil {
		return err
	}

	return tblst.createTable(ctx, tx, tn, mid, ts)
}

func (tblst *tableStore) DropTable(ctx context.Context, tx storage.Transaction, tn sql.TableName,
	ifExists bool) error {

	err := tblst.updateSchema(ctx, tx, tn.SchemaName(), -1)
	if err != nil {
		return err
	}

	tbl, err := tblst.tables.Table(ctx, tx)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(tablesTableName, tbl)

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
		return fmt.Errorf("%s: table %s not found", tblst.name, tn)
	} else if err != nil {
		return err
	}

	return rows.Delete(ctx)
}

func (tblst *tableStore) lookupIndex(ctx context.Context, tx storage.Transaction, tn sql.TableName,
	idxname sql.Identifier) (bool, error) {

	tbl, err := tblst.indexes.Table(ctx, tx)
	if err != nil {
		return false, err
	}
	ttbl := util.MakeTypedTable(indexesTableName, tbl)

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

func (tblst *tableStore) CreateIndex(ctx context.Context, tx storage.Transaction,
	idxname sql.Identifier, tn sql.TableName, unique bool, keys []sql.ColumnKey,
	ifNotExists bool) error {

	ok, err := tblst.lookupIndex(ctx, tx, tn, idxname)
	if err != nil {
		return err
	}
	if ok {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("%s: index %s on table %s already exists", tblst.name, idxname, tn)
	}

	ok, err = tblst.validTable(ctx, tx, tn)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s: table %s not found", tblst.name, tn)
	}

	tbl, err := tblst.indexes.Table(ctx, tx)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(indexesTableName, tbl)

	return ttbl.Insert(ctx,
		indexRow{
			Database: tn.Database.String(),
			Schema:   tn.Schema.String(),
			Table:    tn.Table.String(),
			Index:    idxname.String(),
		})
}

func (tblst *tableStore) DropIndex(ctx context.Context, tx storage.Transaction,
	idxname sql.Identifier, tn sql.TableName, ifExists bool) error {

	tbl, err := tblst.indexes.Table(ctx, tx)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(indexesTableName, tbl)

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
		return fmt.Errorf("%s: index %s on table %s not found", tblst.name, idxname, tn)
	} else if err != nil {
		return err
	}

	return rows.Delete(ctx)
}

func (tblst *tableStore) Begin(sesid uint64) storage.Transaction {
	return tblst.st.Begin(sesid)
}

func (tblst *tableStore) ListDatabases(ctx context.Context,
	tx storage.Transaction) ([]sql.Identifier, error) {

	tbl, err := tblst.databases.Table(ctx, tx)
	if err != nil {
		return nil, err
	}
	ttbl := util.MakeTypedTable(databasesTableName, tbl)

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

func (tblst *tableStore) ListSchemas(ctx context.Context, tx storage.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	tbl, err := tblst.schemas.Table(ctx, tx)
	if err != nil {
		return nil, err
	}
	ttbl := util.MakeTypedTable(schemasTableName, tbl)

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

func (tblst *tableStore) ListTables(ctx context.Context, tx storage.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	tbl, err := tblst.tables.Table(ctx, tx)
	if err != nil {
		return nil, err
	}
	ttbl := util.MakeTypedTable(tablesTableName, tbl)

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

func (tblst *tableStore) nextSequenceValue(ctx context.Context, tx storage.Transaction,
	sequence string) (int64, error) {

	tbl, err := tblst.sequences.Table(ctx, tx)
	if err != nil {
		return 0, err
	}
	ttbl := util.MakeTypedTable(sequencesTableName, tbl)

	keyRow := sequenceRow{Sequence: sequence}
	rows, err := ttbl.Rows(ctx, keyRow, keyRow)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var sr sequenceRow
	err = rows.Next(ctx, &sr)
	if err != nil {
		return 0, fmt.Errorf("%s: sequence %s not found", tblst.name, sequence)
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
