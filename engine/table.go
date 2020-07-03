package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

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

func convertValue(ct sql.ColumnType, n sql.Identifier, v sql.Value) (sql.Value, error) {
	if v == nil {
		if ct.NotNull {
			return nil, fmt.Errorf("column \"%s\" may not be NULL", n)
		}
		return nil, nil
	}

	switch ct.Type {
	case sql.BooleanType:
		if sv, ok := v.(sql.StringValue); ok {
			s := strings.Trim(string(sv), " \t\n")
			if s == "t" || s == "true" || s == "y" || s == "yes" || s == "on" || s == "1" {
				return sql.BoolValue(true), nil
			} else if s == "f" || s == "false" || s == "n" || s == "no" || s == "off" || s == "0" {
				return sql.BoolValue(false), nil
			} else {
				return nil, fmt.Errorf("column \"%s\": expected a boolean value: %v", n, v)
			}
		} else if _, ok := v.(sql.BoolValue); !ok {
			return nil, fmt.Errorf("column \"%s\": expected a boolean value: %v", n, v)
		}
	case sql.StringType:
		if i, ok := v.(sql.Int64Value); ok {
			return sql.StringValue(strconv.FormatInt(int64(i), 10)), nil
		} else if f, ok := v.(sql.Float64Value); ok {
			return sql.StringValue(strconv.FormatFloat(float64(f), 'g', -1, 64)), nil
		} else if b, ok := v.(sql.BytesValue); ok {
			if !utf8.Valid([]byte(b)) {
				return nil, fmt.Errorf(`column "%s": expected a valid utf8 string: %v`, n, v)
			}
			return sql.StringValue(b), nil
		} else if _, ok := v.(sql.StringValue); !ok {
			return nil, fmt.Errorf(`column "%s": expected a string value: %v`, n, v)
		}
	case sql.BytesType:
		if s, ok := v.(sql.StringValue); ok {
			return sql.BytesValue(s), nil
		} else if _, ok := v.(sql.BytesValue); !ok {
			return nil, fmt.Errorf(`column "%s": expected a bytes value: %v`, n, v)
		}
	case sql.FloatType:
		if i, ok := v.(sql.Int64Value); ok {
			return sql.Float64Value(i), nil
		} else if s, ok := v.(sql.StringValue); ok {
			d, err := strconv.ParseFloat(strings.Trim(string(s), " \t\n"), 64)
			if err != nil {
				return nil, fmt.Errorf("column \"%s\": expected a float: %v: %s", n, v, err)
			}
			return sql.Float64Value(d), nil
		} else if _, ok := v.(sql.Float64Value); !ok {
			return nil, fmt.Errorf("column \"%s\": expected a float value: %v", n, v)
		}
	case sql.IntegerType:
		if f, ok := v.(sql.Float64Value); ok {
			return sql.Int64Value(f), nil
		} else if s, ok := v.(sql.StringValue); ok {
			i, err := strconv.ParseInt(strings.Trim(string(s), " \t\n"), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("column \"%s\": expected an integer: %v: %s", n, v, err)
			}
			return sql.Int64Value(i), nil
		} else if _, ok := v.(sql.Int64Value); !ok {
			return nil, fmt.Errorf("column \"%s\": expected an integer value: %v", n, v)
		}
	}

	return v, nil
}

func (tbl *table) Insert(ctx context.Context, row []sql.Value) error {
	cols := tbl.tt.Columns()
	for rdx, ct := range tbl.tt.ColumnTypes() {
		var err error
		row[rdx], err = convertValue(ct, cols[rdx], row[rdx])
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
		up.Value, err = convertValue(ct, cols[up.Index], up.Value)
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
