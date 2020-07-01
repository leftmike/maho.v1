package engine

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/sql"
)

type table struct {
	tn sql.TableName
	st sql.Table
	tt sql.TableType
}

type rows struct {
	tbl  *table
	rows sql.Rows
}

func makeTable(tn sql.TableName, st sql.Table, tt sql.TableType) (*table, sql.TableType, error) {
	return &table{
		tn: tn,
		st: st,
		tt: tt,
	}, tt, nil
}

func (tbl *table) Columns(ctx context.Context) []sql.Identifier {
	return tbl.tt.Columns()
}

func (tbl *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return tbl.tt.ColumnTypes()
}

func (tbl *table) PrimaryKey(ctx context.Context) []sql.ColumnKey {
	return tbl.tt.PrimaryKey()
}

func (tbl *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (sql.Rows, error) {
	r, err := tbl.st.Rows(ctx, minRow, maxRow)
	if err != nil {
		return nil, err
	}
	return &rows{
		tbl:  tbl,
		rows: r,
	}, nil
}

func (tbl *table) Insert(ctx context.Context, row []sql.Value) error {
	cols := tbl.tt.Columns()
	for rdx, ct := range tbl.tt.ColumnTypes() {
		var err error
		row[rdx], err = ct.ConvertValue(cols[rdx], row[rdx])
		if err != nil {
			return fmt.Errorf("engine: table %s: %s", tbl.tn, err)
		}
	}
	return tbl.st.Insert(ctx, row)
}

func (tbl *table) update(ctx context.Context, r sql.Rows, updates []sql.ColumnUpdate) error {
	cols := tbl.tt.Columns()
	colTypes := tbl.tt.ColumnTypes()
	for _, up := range updates {
		ct := colTypes[up.Index]

		var err error
		up.Value, err = ct.ConvertValue(cols[up.Index], up.Value)
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
