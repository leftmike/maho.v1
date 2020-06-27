package mideng

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

type TableDef interface {
	Table(ctx context.Context, tx storage.Transaction) (storage.Table, error)
	Columns() []sql.Identifier
	ColumnTypes() []sql.ColumnType
	PrimaryKey() []sql.ColumnKey
}

type Transaction interface {
	storage.Transaction
	Changes(cfn func(mid int64, key string, row []sql.Value) bool)
}

type store interface {
	MakeTableDef(tn sql.TableName, mid int64, cols []sql.Identifier, colTypes []sql.ColumnType,
		primary []sql.ColumnKey) (TableDef, error)
	Begin(sesid uint64) Transaction
}

type midStore struct {
	name      string
	st        store
	sequences TableDef
	databases TableDef
	schemas   TableDef
	tables    TableDef
	indexes   TableDef
}

func NewStore(name string, st store, init bool) (storage.Store, error) {
	sequences, err := st.MakeTableDef(sequencesTableName, sequencesMID,
		[]sql.Identifier{sql.ID("sequence"), sql.ID("current")},
		[]sql.ColumnType{sql.IdColType, sql.Int64ColType},
		[]sql.ColumnKey{sql.MakeColumnKey(0, false)})
	if err != nil {
		return nil, err
	}
	databases, err := st.MakeTableDef(databasesTableName, databasesMID,
		[]sql.Identifier{sql.ID("database")},
		[]sql.ColumnType{sql.IdColType},
		[]sql.ColumnKey{sql.MakeColumnKey(0, false)})
	if err != nil {
		return nil, err
	}
	schemas, err := st.MakeTableDef(schemasTableName, schemasMID,
		[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("tables")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.Int64ColType},
		[]sql.ColumnKey{sql.MakeColumnKey(0, false), sql.MakeColumnKey(1, false)})
	if err != nil {
		return nil, err
	}
	tables, err := st.MakeTableDef(tablesTableName, tablesMID,
		[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("table"), sql.ID("mid"),
			sql.ID("metadata")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType, sql.Int64ColType,
			{Type: sql.BytesType, Fixed: false, Size: sql.MaxColumnSize}},
		[]sql.ColumnKey{sql.MakeColumnKey(0, false), sql.MakeColumnKey(1, false),
			sql.MakeColumnKey(2, false)})
	if err != nil {
		return nil, err
	}
	indexes, err := st.MakeTableDef(indexesTableName, indexesMID,
		[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("table"), sql.ID("index")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType, sql.IdColType},
		[]sql.ColumnKey{sql.MakeColumnKey(0, false), sql.MakeColumnKey(1, false),
			sql.MakeColumnKey(2, false), sql.MakeColumnKey(3, false)})
	if err != nil {
		return nil, err
	}

	mst := &midStore{
		name:      name,
		st:        st,
		sequences: sequences,
		databases: databases,
		schemas:   schemas,
		tables:    tables,
		indexes:   indexes,
	}
	if init {
		ctx := context.Background()
		tx := mst.st.Begin(0)
		err = mst.init(ctx, tx)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}
	}

	return mst, nil
}

func (mst *midStore) init(ctx context.Context, tx Transaction) error {
	tbl, err := mst.sequences.Table(ctx, tx)
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

	tbl, err = mst.databases.Table(ctx, tx)
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

	tbl, err = mst.schemas.Table(ctx, tx)
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

	/*
		tx.Changes(
			func(mid int64, key string, row []sql.Value) bool {
				fmt.Printf("%d: %s: %v\n", mid, key, row)
				return true
			})
	*/

	tx.NextStmt()
	err = mst.createTable(ctx, tx, sequencesTableName, sequencesMID, mst.sequences)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = mst.createTable(ctx, tx, databasesTableName, databasesMID, mst.databases)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = mst.createTable(ctx, tx, schemasTableName, schemasMID, mst.schemas)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = mst.createTable(ctx, tx, tablesTableName, tablesMID, mst.tables)
	if err != nil {
		return err
	}

	tx.NextStmt()
	err = mst.createTable(ctx, tx, indexesTableName, indexesMID, mst.indexes)
	if err != nil {
		return err
	}

	return nil
}

func (mst *midStore) createDatabase(ctx context.Context, tx storage.Transaction,
	dbname sql.Identifier) error {

	tbl, err := mst.databases.Table(ctx, tx)
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
	return mst.CreateSchema(ctx, tx, sql.SchemaName{dbname, sql.PUBLIC})
}

func (mst *midStore) CreateDatabase(dbname sql.Identifier,
	options map[sql.Identifier]string) error {

	if len(options) != 0 {
		return fmt.Errorf("%s: unexpected option to create database: %s", mst.name, dbname)
	}

	ctx := context.Background()
	tx := mst.st.Begin(0)
	err := mst.createDatabase(ctx, tx, dbname)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

func (mst *midStore) lookupDatabase(ctx context.Context, tx storage.Transaction,
	dbname sql.Identifier) (*util.Rows, error) {

	tbl, err := mst.databases.Table(ctx, tx)
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

func (mst *midStore) validDatabase(ctx context.Context, tx storage.Transaction,
	dbname sql.Identifier) (bool, error) {

	rows, err := mst.lookupDatabase(ctx, tx, dbname)
	if err != nil {
		return false, err
	}
	if rows == nil {
		return false, nil
	}
	rows.Close()
	return true, nil
}

func (mst *midStore) dropDatabase(ctx context.Context, tx storage.Transaction,
	dbname sql.Identifier, ifExists bool) error {

	rows, err := mst.lookupDatabase(ctx, tx, dbname)
	if err != nil {
		return err
	}
	if rows == nil {
		if ifExists {
			return nil
		}
		return fmt.Errorf("%s: database %s does not exist", mst.name, dbname)
	}
	defer rows.Close()

	scnames, err := mst.ListSchemas(ctx, tx, dbname)
	if err != nil {
		return err
	}
	for _, scname := range scnames {
		err = mst.DropSchema(ctx, tx, sql.SchemaName{dbname, scname}, true)
		if err != nil {
			return err
		}
	}

	return rows.Delete(ctx)
}

func (mst *midStore) DropDatabase(dbname sql.Identifier, ifExists bool,
	options map[sql.Identifier]string) error {

	if len(options) != 0 {
		return fmt.Errorf("%s: unexpected option to drop database: %s", mst.name, dbname)
	}

	ctx := context.Background()
	tx := mst.st.Begin(0)
	err := mst.dropDatabase(ctx, tx, dbname, ifExists)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

func (mst *midStore) CreateSchema(ctx context.Context, tx storage.Transaction,
	sn sql.SchemaName) error {

	ok, err := mst.validDatabase(ctx, tx, sn.Database)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s: database %s not found", mst.name, sn.Database)
	}

	tbl, err := mst.schemas.Table(ctx, tx)
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

func (mst *midStore) DropSchema(ctx context.Context, tx storage.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	tbl, err := mst.schemas.Table(ctx, tx)
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
		return fmt.Errorf("%s: schema %s not found", mst.name, sn)
	} else if err != nil {
		return err
	}
	if sr.Tables > 0 {
		return fmt.Errorf("%s: schema %s is not empty", mst.name, sn)
	}
	return rows.Delete(ctx)
}

func (mst *midStore) updateSchema(ctx context.Context, tx storage.Transaction, sn sql.SchemaName,
	delta int64) error {

	tbl, err := mst.schemas.Table(ctx, tx)
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
		return fmt.Errorf("%s: schema %s not found", mst.name, sn)
	} else if err != nil {
		return err
	}

	return rows.Update(ctx,
		struct {
			Tables int64
		}{sr.Tables + delta})
}

func (mst *midStore) lookupTable(ctx context.Context, tx storage.Transaction,
	tn sql.TableName) (*util.Rows, error) {

	tbl, err := mst.tables.Table(ctx, tx)
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

func (mst *midStore) validTable(ctx context.Context, tx storage.Transaction,
	tn sql.TableName) (bool, error) {

	rows, err := mst.lookupTable(ctx, tx, tn)
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

func (mst *midStore) decodeTableMetadata(tn sql.TableName, mid int64, buf []byte) (TableDef,
	error) {

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

	return mst.st.MakeTableDef(tn, mid, cols, colTypes, primary)
}

func (mst *midStore) LookupTable(ctx context.Context, tx storage.Transaction,
	tn sql.TableName) (storage.Table, error) {

	rows, err := mst.lookupTable(ctx, tx, tn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tr tableRow
	err = rows.Next(ctx, &tr)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("%s: table %s not found", mst.name, tn)
		}
		return nil, err
	}
	mid := tr.MID

	td, err := mst.decodeTableMetadata(tn, mid, tr.Metadata)
	if err != nil {
		return nil, err
	}

	return td.Table(ctx, tx)
}

func encodeTableMetadata(td TableDef) ([]byte, error) {
	cols := td.Columns()
	colTypes := td.ColumnTypes()

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

	primary := td.PrimaryKey()
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

func (mst *midStore) createTable(ctx context.Context, tx storage.Transaction, tn sql.TableName,
	mid int64, td TableDef) error {

	err := mst.updateSchema(ctx, tx, tn.SchemaName(), 1)
	if err != nil {
		return err
	}

	md, err := encodeTableMetadata(td)
	if err != nil {
		return err
	}

	tbl, err := mst.tables.Table(ctx, tx)
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

func (mst *midStore) CreateTable(ctx context.Context, tx storage.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []sql.ColumnKey,
	ifNotExists bool) error {

	if len(primary) == 0 {
		rowID := sql.ID("rowid")

		for _, col := range cols {
			if col == rowID {
				return fmt.Errorf("%s: unable to add %s column for table %s missing primary key",
					mst.name, rowID, tn)
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

	ok, err := mst.validTable(ctx, tx, tn)
	if err != nil {
		return err
	}
	if ok {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("%s: table %s already exists", mst.name, tn)
	}

	mid, err := mst.nextSequenceValue(ctx, tx, midSequence)
	if err != nil {
		return err
	}

	td, err := mst.st.MakeTableDef(tn, mid, cols, colTypes, primary)
	if err != nil {
		return err
	}

	return mst.createTable(ctx, tx, tn, mid, td)
}

func (mst *midStore) DropTable(ctx context.Context, tx storage.Transaction, tn sql.TableName,
	ifExists bool) error {

	err := mst.updateSchema(ctx, tx, tn.SchemaName(), -1)
	if err != nil {
		return err
	}

	tbl, err := mst.tables.Table(ctx, tx)
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
		return fmt.Errorf("%s: table %s not found", mst.name, tn)
	} else if err != nil {
		return err
	}

	return rows.Delete(ctx)
}

func (mst *midStore) lookupIndex(ctx context.Context, tx storage.Transaction, tn sql.TableName,
	idxname sql.Identifier) (bool, error) {

	tbl, err := mst.indexes.Table(ctx, tx)
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

func (mst *midStore) CreateIndex(ctx context.Context, tx storage.Transaction,
	idxname sql.Identifier, tn sql.TableName, unique bool, keys []sql.ColumnKey,
	ifNotExists bool) error {

	ok, err := mst.lookupIndex(ctx, tx, tn, idxname)
	if err != nil {
		return err
	}
	if ok {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("%s: index %s on table %s already exists", mst.name, idxname, tn)
	}

	ok, err = mst.validTable(ctx, tx, tn)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s: table %s not found", mst.name, tn)
	}

	tbl, err := mst.indexes.Table(ctx, tx)
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

func (mst *midStore) DropIndex(ctx context.Context, tx storage.Transaction,
	idxname sql.Identifier, tn sql.TableName, ifExists bool) error {

	tbl, err := mst.indexes.Table(ctx, tx)
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
		return fmt.Errorf("%s: index %s on table %s not found", mst.name, idxname, tn)
	} else if err != nil {
		return err
	}

	return rows.Delete(ctx)
}

func (mst *midStore) Begin(sesid uint64) storage.Transaction {
	return mst.st.Begin(sesid)
}

func (mst *midStore) ListDatabases(ctx context.Context, tx storage.Transaction) ([]sql.Identifier,
	error) {

	tbl, err := mst.databases.Table(ctx, tx)
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

func (mst *midStore) ListSchemas(ctx context.Context, tx storage.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	tbl, err := mst.schemas.Table(ctx, tx)
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

func (mst *midStore) ListTables(ctx context.Context, tx storage.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	tbl, err := mst.tables.Table(ctx, tx)
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

func (mst *midStore) nextSequenceValue(ctx context.Context, tx storage.Transaction,
	sequence string) (int64, error) {

	tbl, err := mst.sequences.Table(ctx, tx)
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
		return 0, fmt.Errorf("%s: sequence %s not found", mst.name, sequence)
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
