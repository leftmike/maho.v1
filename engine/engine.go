package engine

import (
	"context"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

type Options storage.Options

type Transaction interface {
	storage.Transaction
}

type MakeVirtual storage.MakeVirtual

type ColumnKey storage.ColumnKey

func MakeColumnKey(num int, reverse bool) ColumnKey {
	return (ColumnKey)(storage.MakeColumnKey(num, reverse))
}

type Rows interface {
	storage.Rows
}

type Table interface {
	storage.Table
}

type Engine struct {
	st storage.Store
}

func NewEngine(st storage.Store) Engine {
	return Engine{
		st: st,
	}
}

func (e Engine) CreateSystemInfoTable(tblname sql.Identifier, maker MakeVirtual) {
	e.st.CreateSystemInfoTable(tblname, (storage.MakeVirtual)(maker))
}

func (e Engine) CreateMetadataTable(tblname sql.Identifier, maker MakeVirtual) {
	e.st.CreateMetadataTable(tblname, (storage.MakeVirtual)(maker))
}

func (e Engine) CreateDatabase(dbname sql.Identifier, options Options) error {
	return e.st.CreateDatabase(dbname, (storage.Options)(options))
}

func (e Engine) DropDatabase(dbname sql.Identifier, ifExists bool, options Options) error {
	return e.st.DropDatabase(dbname, ifExists, (storage.Options)(options))
}

func (e Engine) CreateSchema(ctx context.Context, tx Transaction, sn sql.SchemaName) error {
	return e.st.CreateSchema(ctx, tx, sn)
}

func (e Engine) DropSchema(ctx context.Context, tx Transaction, sn sql.SchemaName,
	ifExists bool) error {

	return e.st.DropSchema(ctx, tx, sn, ifExists)
}

func (e Engine) LookupTable(ctx context.Context, tx Transaction, tn sql.TableName) (Table, error) {
	return e.st.LookupTable(ctx, tx, tn)
}

func columnKey(keys []ColumnKey) []storage.ColumnKey {
	skeys := make([]storage.ColumnKey, len(keys))
	for kdx, key := range keys {
		skeys[kdx] = (storage.ColumnKey)(key)
	}
	return skeys
}

func (e Engine) CreateTable(ctx context.Context, tx Transaction, tn sql.TableName,
	cols []sql.Identifier, colTypes []sql.ColumnType, primary []ColumnKey, ifNotExists bool) error {

	return e.st.CreateTable(ctx, tx, tn, cols, colTypes, columnKey(primary), ifNotExists)
}

func (e Engine) DropTable(ctx context.Context, tx Transaction, tn sql.TableName,
	ifExists bool) error {

	return e.st.DropTable(ctx, tx, tn, ifExists)
}

func (e Engine) CreateIndex(ctx context.Context, tx Transaction, idxname sql.Identifier,
	tn sql.TableName, unique bool, keys []ColumnKey, ifNotExists bool) error {

	return e.st.CreateIndex(ctx, tx, idxname, tn, unique, columnKey(keys), ifNotExists)
}

func (e Engine) DropIndex(ctx context.Context, tx Transaction, idxname sql.Identifier,
	tn sql.TableName, ifExists bool) error {

	return e.st.DropIndex(ctx, tx, idxname, tn, ifExists)
}

func (e Engine) Begin(sesid uint64) Transaction {
	return e.st.Begin(sesid)
}

func (e Engine) ListDatabases(ctx context.Context, tx Transaction) ([]sql.Identifier, error) {
	return e.st.ListDatabases(ctx, tx)
}

func (e Engine) ListSchemas(ctx context.Context, tx Transaction,
	dbname sql.Identifier) ([]sql.Identifier, error) {

	return e.st.ListSchemas(ctx, tx, dbname)
}

func (e Engine) ListTables(ctx context.Context, tx Transaction,
	sn sql.SchemaName) ([]sql.Identifier, error) {

	return e.st.ListTables(ctx, tx, sn)
}
