package util

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type utilEngine interface {
	Name() string
	AllocateMID(ctx context.Context) (uint64, error)
	MakeSchemasTable(etx engine.Transaction) *TypedTable
	MakeTablesTable(etx engine.Transaction) *TypedTable
	MakeIndexesTable(etx engine.Transaction) *TypedTable
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
}

type indexRow struct {
	Database string
	Schema   string
	Table    string
	Index    string
}

var (
	SchemasColumns     = []sql.Identifier{sql.ID("database"), sql.ID("schema"), sql.ID("tables")}
	SchemasColumnTypes = []sql.ColumnType{sql.IdColType, sql.IdColType, sql.Int64ColType}
	SchemasPrimaryKey  = []engine.ColumnKey{
		engine.MakeColumnKey(0, false),
		engine.MakeColumnKey(1, false),
	}

	TablesColumns = []sql.Identifier{
		sql.ID("database"),
		sql.ID("schema"),
		sql.ID("table"),
		sql.ID("mid"),
	}
	TablesColumnTypes = []sql.ColumnType{
		sql.IdColType,
		sql.IdColType,
		sql.IdColType,
		sql.Int64ColType,
	}
	TablesPrimaryKey = []engine.ColumnKey{
		engine.MakeColumnKey(0, false),
		engine.MakeColumnKey(1, false),
		engine.MakeColumnKey(2, false),
	}

	IndexesColumns = []sql.Identifier{
		sql.ID("database"),
		sql.ID("schema"),
		sql.ID("table"),
		sql.ID("index"),
	}
	IndexesColumnTypes = []sql.ColumnType{
		sql.IdColType,
		sql.IdColType,
		sql.IdColType,
		sql.IdColType,
	}
	IndexesPrimaryKey = []engine.ColumnKey{
		engine.MakeColumnKey(0, false),
		engine.MakeColumnKey(1, false),
		engine.MakeColumnKey(2, false),
		engine.MakeColumnKey(3, false),
	}
)

func CreateSchema(ctx context.Context, ue utilEngine, etx engine.Transaction,
	sn sql.SchemaName) error {

	ttbl := ue.MakeSchemasTable(etx)
	return ttbl.Insert(ctx,
		schemaRow{
			Database: sn.Database.String(),
			Schema:   sn.Schema.String(),
			Tables:   0,
		})
}

func DropSchema(ctx context.Context, ue utilEngine, etx engine.Transaction, sn sql.SchemaName,
	ifExists bool) error {

	ttbl := ue.MakeSchemasTable(etx)
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
		return fmt.Errorf("%s: schema %s not found", ue.Name(), sn)
	} else if err != nil {
		return err
	}

	if sr.Database != sn.Database.String() || sr.Schema != sn.Schema.String() {
		if ifExists {
			return nil
		}
		return fmt.Errorf("%s: schema %s not found", ue.Name(), sn)
	}
	if sr.Tables > 0 {
		return fmt.Errorf("%s: schema %s is not empty", ue.Name(), sn)
	}
	return rows.Delete(ctx)
}

func updateSchema(ctx context.Context, ue utilEngine, etx engine.Transaction, sn sql.SchemaName,
	delta int64) error {

	ttbl := ue.MakeSchemasTable(etx)
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
		return fmt.Errorf("%s: schema %s not found", ue.Name(), sn)
	} else if err != nil {
		return err
	}

	if sr.Database != sn.Database.String() || sr.Schema != sn.Schema.String() {
		return fmt.Errorf("%s: schema %s not found", ue.Name(), sn)
	}
	return rows.Update(ctx,
		struct {
			Tables int64
		}{sr.Tables + delta})
}

func LookupTable(ctx context.Context, ue utilEngine, etx engine.Transaction,
	tn sql.TableName) (uint64, error) {

	ttbl := ue.MakeTablesTable(etx)
	keyRow := tableRow{
		Database: tn.Database.String(),
		Schema:   tn.Schema.String(),
		Table:    tn.Table.String(),
	}
	rows, err := ttbl.Rows(ctx, keyRow, keyRow)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var tr tableRow
	err = rows.Next(ctx, &tr)
	if err == io.EOF {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	if tr.Database != tn.Database.String() || tr.Schema != tn.Schema.String() ||
		tr.Table != tn.Table.String() {

		return 0, nil
	}
	return uint64(tr.MID), nil
}

func CreateTable(ctx context.Context, ue utilEngine, etx engine.Transaction, tn sql.TableName,
	ifNotExists bool) (uint64, error) {

	mid, err := LookupTable(ctx, ue, etx, tn)
	if err != nil {
		return 0, err
	}
	if mid > 0 {
		if ifNotExists {
			return 0, nil
		}
		return 0, fmt.Errorf("%s: table %s already exists", ue.Name(), tn)
	}

	err = updateSchema(ctx, ue, etx, tn.SchemaName(), 1)
	if err != nil {
		return 0, err
	}

	mid, err = ue.AllocateMID(ctx)
	if err != nil {
		return 0, err
	}

	ttbl := ue.MakeTablesTable(etx)
	err = ttbl.Insert(ctx,
		tableRow{
			Database: tn.Database.String(),
			Schema:   tn.Schema.String(),
			Table:    tn.Table.String(),
			MID:      int64(mid),
		})
	if err != nil {
		return 0, err
	}
	return mid, nil
}

func DropTable(ctx context.Context, ue utilEngine, etx engine.Transaction, tn sql.TableName,
	ifExists bool) error {

	err := updateSchema(ctx, ue, etx, tn.SchemaName(), -1)
	if err != nil {
		return err
	}

	ttbl := ue.MakeTablesTable(etx)
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
		return fmt.Errorf("%s: table %s not found", ue.Name(), tn)
	} else if err != nil {
		return err
	}

	if tr.Database != tn.Database.String() || tr.Schema != tn.Schema.String() ||
		tr.Table != tn.Table.String() {

		if ifExists {
			return nil
		}
		return fmt.Errorf("%s: table %s not found", ue.Name(), tn)
	}
	return rows.Delete(ctx)
}

func lookupIndex(ctx context.Context, ue utilEngine, etx engine.Transaction, tn sql.TableName,
	idxname sql.Identifier) (bool, error) {

	ttbl := ue.MakeIndexesTable(etx)
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

	if ir.Database != tn.Database.String() || ir.Schema != tn.Schema.String() ||
		ir.Table != tn.Table.String() || ir.Index != idxname.String() {

		return false, nil
	}
	return true, nil
}

func CreateIndex(ctx context.Context, ue utilEngine, etx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, ifNotExists bool) error {

	ok, err := lookupIndex(ctx, ue, etx, tn, idxname)
	if err != nil {
		return err
	}
	if ok {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("%s: index %s on table %s already exists", ue.Name(), idxname, tn)
	}

	mid, err := LookupTable(ctx, ue, etx, tn)
	if err != nil {
		return err
	}
	if mid == 0 {
		return fmt.Errorf("%s: table %s not found", ue.Name(), tn)
	}

	ttbl := ue.MakeIndexesTable(etx)
	return ttbl.Insert(ctx,
		indexRow{
			Database: tn.Database.String(),
			Schema:   tn.Schema.String(),
			Table:    tn.Table.String(),
			Index:    idxname.String(),
		})
	return nil
}

func DropIndex(ctx context.Context, ue utilEngine, etx engine.Transaction,
	idxname sql.Identifier, tn sql.TableName, ifExists bool) error {

	ttbl := ue.MakeIndexesTable(etx)
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
		return fmt.Errorf("%s: index %s on table %s not found", ue.Name(), idxname, tn)
	} else if err != nil {
		return err
	}

	if ir.Database != tn.Database.String() || ir.Schema != tn.Schema.String() ||
		ir.Table != tn.Table.String() || ir.Index != idxname.String() {

		if ifExists {
			return nil
		}
		return fmt.Errorf("%s: index %s on table %s not found", ue.Name(), idxname, tn)
	}
	return rows.Delete(ctx)
}

func ListSchemas(ctx context.Context, ue utilEngine, etx engine.Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	ttbl := ue.MakeSchemasTable(etx)
	rows, err := ttbl.Rows(ctx, schemaRow{Database: dbname.String()}, nil)
	if err != nil {
		return nil, err
	}

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

func ListTables(ctx context.Context, ue utilEngine, etx engine.Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	ttbl := ue.MakeTablesTable(etx)
	rows, err := ttbl.Rows(ctx,
		tableRow{Database: sn.Database.String(), Table: sn.Schema.String()}, nil)
	if err != nil {
		return nil, err
	}

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
