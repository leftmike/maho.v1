package mideng

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/util"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

const (
	sequencesMID = 1
	databasesMID = 2
	schemasMID   = 3
	tablesMID    = 4
	indexesMID   = 5

	midSequence = "mid"
)

var (
	sequencesTableName = sql.TableName{sql.ID("system"), sql.ID("private"), sql.ID("sequences")}
	databasesTableName = sql.TableName{sql.ID("system"), sql.ID("private"), sql.ID("databases")}
	schemasTableName   = sql.TableName{sql.ID("system"), sql.ID("private"), sql.ID("schemas")}
	tablesTableName    = sql.TableName{sql.ID("system"), sql.ID("private"), sql.ID("tables")}
	indexesTableName   = sql.TableName{sql.ID("system"), sql.ID("private"), sql.ID("indexes")}
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
	Table(ctx context.Context, tx engine.Transaction) (engine.Table, error)
	Encode() ([]byte, error)
}

type store interface {
	MakeTableDef(tn sql.TableName, mid int64, cols []sql.Identifier, colTypes []sql.ColumnType,
		primary []engine.ColumnKey) (TableDef, error)
	DecodeTableDef(tn sql.TableName, mid int64, buf []byte) (TableDef, error)
	Begin(sesid uint64) engine.Transaction
}

type midEngine struct {
	name      string
	st        store
	sequences TableDef
	databases TableDef
	schemas   TableDef
	tables    TableDef
	indexes   TableDef
}

func NewEngine(name string, st store, init bool) (engine.Engine, error) {
	sequences, err := st.MakeTableDef(sequencesTableName, sequencesMID,
		[]sql.Identifier{sql.ID("sequence"), sql.ID("current")},
		[]sql.ColumnType{sql.IdColType, sql.Int64ColType},
		[]engine.ColumnKey{engine.MakeColumnKey(0, false)})
	if err != nil {
		return nil, err
	}
	databases, err := st.MakeTableDef(databasesTableName, databasesMID,
		[]sql.Identifier{sql.ID("database")},
		[]sql.ColumnType{sql.IdColType},
		[]engine.ColumnKey{engine.MakeColumnKey(0, false)})
	if err != nil {
		return nil, err
	}
	schemas, err := st.MakeTableDef(schemasTableName, schemasMID,
		[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("tables")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.Int64ColType},
		[]engine.ColumnKey{engine.MakeColumnKey(0, false), engine.MakeColumnKey(1, false)})
	if err != nil {
		return nil, err
	}
	tables, err := st.MakeTableDef(tablesTableName, tablesMID,
		[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("table"), sql.ID("mid"),
			sql.ID("metadata")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType, sql.Int64ColType,
			{Type: sql.BytesType, Fixed: false, Size: sql.MaxColumnSize}},
		[]engine.ColumnKey{engine.MakeColumnKey(0, false), engine.MakeColumnKey(1, false),
			engine.MakeColumnKey(2, false)})
	if err != nil {
		return nil, err
	}
	indexes, err := st.MakeTableDef(indexesTableName, indexesMID,
		[]sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("table"), sql.ID("index")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.IdColType, sql.IdColType},
		[]engine.ColumnKey{engine.MakeColumnKey(0, false), engine.MakeColumnKey(1, false),
			engine.MakeColumnKey(2, false), engine.MakeColumnKey(3, false)})
	if err != nil {
		return nil, err
	}

	me := &midEngine{
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
		tx := me.st.Begin(0)
		err = me.init(ctx, tx)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}
	}
	return me, nil
}

func (me *midEngine) CreateSystemTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic(fmt.Sprintf("%s: use virtual engine with %s engine", me.name, me.name))
}

func (me *midEngine) CreateInfoTable(tblname sql.Identifier, maker engine.MakeVirtual) {
	panic(fmt.Sprintf("%s: use virtual engine with %s engine", me.name, me.name))
}

func (me *midEngine) init(ctx context.Context, tx engine.Transaction) error {
	tbl, err := me.sequences.Table(ctx, tx)
	if err != nil {
		return err
	}
	ttbl := util.MakeTypedTable(sequencesTableName, tbl)

	return ttbl.Insert(ctx,
		sequenceRow{
			Sequence: midSequence,
			Current:  2048,
		})
	return nil
}

func (me *midEngine) createDatabase(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) error {

	tbl, err := me.databases.Table(ctx, tx)
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

	return me.CreateSchema(ctx, tx, sql.SchemaName{dbname, sql.PUBLIC})
}

func (me *midEngine) CreateDatabase(dbname sql.Identifier, options engine.Options) error {
	if len(options) != 0 {
		return fmt.Errorf("%s: unexpected option to create database: %s", me.name, dbname)
	}

	ctx := context.Background()
	tx := me.st.Begin(0)
	err := me.createDatabase(ctx, tx, dbname)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

func (me *midEngine) lookupDatabase(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) (*util.Rows, error) {

	tbl, err := me.databases.Table(ctx, tx)
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

func (me *midEngine) validDatabase(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) (bool, error) {

	rows, err := me.lookupDatabase(ctx, tx, dbname)
	if err != nil {
		return false, err
	}
	if rows == nil {
		return false, nil
	}
	rows.Close()
	return true, nil
}

func (me *midEngine) dropDatabase(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier, ifExists bool) error {

	rows, err := me.lookupDatabase(ctx, tx, dbname)
	if err != nil {
		return err
	}
	if rows == nil {
		if ifExists {
			return nil
		}
		return fmt.Errorf("%s: database %s does not exist", me.name, dbname)
	}
	defer rows.Close()

	scnames, err := me.ListSchemas(ctx, tx, dbname)
	if err != nil {
		return err
	}
	for _, scname := range scnames {
		err = me.DropSchema(ctx, tx, sql.SchemaName{dbname, scname}, true)
		if err != nil {
			return err
		}
	}

	return rows.Delete(ctx)
}

func (me *midEngine) DropDatabase(dbname sql.Identifier, ifExists bool,
	options engine.Options) error {

	if len(options) != 0 {
		return fmt.Errorf("%s: unexpected option to drop database: %s", me.name, dbname)
	}

	ctx := context.Background()
	tx := me.st.Begin(0)
	err := me.dropDatabase(ctx, tx, dbname, ifExists)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}

func (me *midEngine) CreateSchema(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) error {

	ok, err := me.validDatabase(ctx, tx, sn.Database)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s: database %s not found", me.name, sn.Database)
	}

	tbl, err := me.schemas.Table(ctx, tx)
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

func (me *midEngine) DropSchema(ctx context.Context, tx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	tbl, err := me.schemas.Table(ctx, tx)
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
		return fmt.Errorf("%s: schema %s not found", me.name, sn)
	} else if err != nil {
		return err
	}
	if sr.Tables > 0 {
		return fmt.Errorf("%s: schema %s is not empty", me.name, sn)
	}
	return rows.Delete(ctx)
}

func (me *midEngine) updateSchema(ctx context.Context, tx engine.Transaction, sn sql.SchemaName,
	delta int64) error {

	tbl, err := me.schemas.Table(ctx, tx)
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
		return fmt.Errorf("%s: schema %s not found", me.name, sn)
	} else if err != nil {
		return err
	}

	return rows.Update(ctx,
		struct {
			Tables int64
		}{sr.Tables + delta})
}

func (me *midEngine) lookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (*util.Rows, error) {

	tbl, err := me.tables.Table(ctx, tx)
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

func (me *midEngine) validTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (bool, error) {

	rows, err := me.lookupTable(ctx, tx, tn)
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

func (me *midEngine) LookupTable(ctx context.Context, tx engine.Transaction,
	tn sql.TableName) (engine.Table, error) {

	rows, err := me.lookupTable(ctx, tx, tn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tr tableRow
	err = rows.Next(ctx, &tr)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("%s: table %s not found", me.name, tn)
		}
		return nil, err
	}
	mid := tr.MID

	td, err := me.st.DecodeTableDef(tn, mid, tr.Metadata)
	if err != nil {
		return nil, err
	}

	return td.Table(ctx, tx)
}

func (me *midEngine) CreateTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []engine.ColumnKey,
	ifNotExists bool) error {

	if primary == nil {
		rowID := sql.ID("rowid")

		for _, col := range cols {
			if col == rowID {
				return fmt.Errorf("%s: unable to add %s column for table %s missing primary key",
					me.name, rowID, tn)
			}
		}

		primary = []engine.ColumnKey{
			engine.MakeColumnKey(len(cols), false),
		}
		cols = append(cols, rowID)
		colTypes = append(colTypes, sql.ColumnType{
			Type:    sql.IntegerType,
			Size:    8,
			NotNull: true,
			Default: &expr.Call{Name: sql.ID("unique_rowid")},
		})
	}

	ok, err := me.validTable(ctx, tx, tn)
	if err != nil {
		return err
	}
	if ok {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("%s: table %s already exists", me.name, tn)
	}

	err = me.updateSchema(ctx, tx, tn.SchemaName(), 1)
	if err != nil {
		return err
	}

	mid, err := me.nextSequenceValue(ctx, tx, midSequence)
	if err != nil {
		return err
	}

	td, err := me.st.MakeTableDef(tn, mid, cols, colTypes, primary)
	if err != nil {
		return err
	}
	md, err := td.Encode()
	if err != nil {
		return err
	}

	tbl, err := me.tables.Table(ctx, tx)
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

func (me *midEngine) DropTable(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	err := me.updateSchema(ctx, tx, tn.SchemaName(), -1)
	if err != nil {
		return err
	}

	tbl, err := me.tables.Table(ctx, tx)
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
		return fmt.Errorf("%s: table %s not found", me.name, tn)
	} else if err != nil {
		return err
	}

	return rows.Delete(ctx)
}

func (me *midEngine) lookupIndex(ctx context.Context, tx engine.Transaction, tn sql.TableName,
	idxname sql.Identifier) (bool, error) {

	tbl, err := me.indexes.Table(ctx, tx)
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

func (me *midEngine) CreateIndex(ctx context.Context, tx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, unique bool, keys []engine.ColumnKey,
	ifNotExists bool) error {

	ok, err := me.lookupIndex(ctx, tx, tn, idxname)
	if err != nil {
		return err
	}
	if ok {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("%s: index %s on table %s already exists", me.name, idxname, tn)
	}

	ok, err = me.validTable(ctx, tx, tn)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s: table %s not found", me.name, tn)
	}

	tbl, err := me.indexes.Table(ctx, tx)
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

func (me *midEngine) DropIndex(ctx context.Context, tx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, ifExists bool) error {

	tbl, err := me.indexes.Table(ctx, tx)
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
		return fmt.Errorf("%s: index %s on table %s not found", me.name, idxname, tn)
	} else if err != nil {
		return err
	}

	return rows.Delete(ctx)
}

func (me *midEngine) Begin(sesid uint64) engine.Transaction {
	return me.st.Begin(sesid)
}

func (me *midEngine) ListDatabases(ctx context.Context, tx engine.Transaction) ([]sql.Identifier,
	error) {

	tbl, err := me.databases.Table(ctx, tx)
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

func (me *midEngine) ListSchemas(ctx context.Context, tx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	tbl, err := me.schemas.Table(ctx, tx)
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

func (me *midEngine) ListTables(ctx context.Context, tx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	tbl, err := me.tables.Table(ctx, tx)
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

func (me *midEngine) nextSequenceValue(ctx context.Context, tx engine.Transaction,
	sequence string) (int64, error) {

	tbl, err := me.sequences.Table(ctx, tx)
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
		return 0, fmt.Errorf("%s: sequence %s not found", me.name, sequence)
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
