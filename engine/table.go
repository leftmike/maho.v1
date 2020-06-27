package engine

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

type Table interface {
	Columns(ctx context.Context) []sql.Identifier
	ColumnTypes(ctx context.Context) []sql.ColumnType
	PrimaryKey(ctx context.Context) []sql.ColumnKey
	Rows(ctx context.Context, minRow, maxRow []sql.Value) (sql.Rows, error)
	Insert(ctx context.Context, row []sql.Value) error
}

type table struct {
	tn       sql.TableName
	stbl     storage.Table
	cols     []sql.Identifier
	colTypes []sql.ColumnType
	primary  []sql.ColumnKey
}

type rows struct {
	tbl  *table
	rows sql.Rows
}

func makeTable(ctx context.Context, tn sql.TableName, stbl storage.Table) (*table, error) {
	return &table{
		tn:       tn,
		stbl:     stbl,
		cols:     stbl.Columns(ctx),
		colTypes: stbl.ColumnTypes(ctx),
		primary:  stbl.PrimaryKey(ctx),
	}, nil
}

func (tbl *table) Columns(ctx context.Context) []sql.Identifier {
	return tbl.cols
}

func (tbl *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return tbl.colTypes
}

func (tbl *table) PrimaryKey(ctx context.Context) []sql.ColumnKey {
	return tbl.primary
}

func (tbl *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (sql.Rows, error) {
	r, err := tbl.stbl.Rows(ctx, minRow, maxRow)
	if err != nil {
		return nil, err
	}
	return &rows{
		tbl:  tbl,
		rows: r,
	}, nil
}

func (tbl *table) Insert(ctx context.Context, row []sql.Value) error {
	for rdx, ct := range tbl.colTypes {
		var err error
		row[rdx], err = ct.ConvertValue(tbl.cols[rdx], row[rdx])
		if err != nil {
			return fmt.Errorf("engine: table %s: %s", tbl.tn, err)
		}
	}
	return tbl.stbl.Insert(ctx, row)
}

func (tbl *table) update(ctx context.Context, r sql.Rows, updates []sql.ColumnUpdate) error {
	for _, up := range updates {
		ct := tbl.colTypes[up.Index]

		var err error
		up.Value, err = ct.ConvertValue(tbl.cols[up.Index], up.Value)
		if err != nil {
			return fmt.Errorf("engine: table %s: %s", tbl.tn, err)
		}
	}
	return r.Update(ctx, updates)
}

func (r *rows) Columns() []sql.Identifier {
	return r.rows.Columns()
}

func (r *rows) Close() error {
	err := r.rows.Close()
	r.rows = nil
	return err
}

func (r *rows) Next(ctx context.Context, dest []sql.Value) error {
	return r.rows.Next(ctx, dest)
}

func (r *rows) Delete(ctx context.Context) error {
	return r.rows.Delete(ctx)
}

func (r *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	return r.tbl.update(ctx, r.rows, updates)
}
