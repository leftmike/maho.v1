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
	st Table
	tt *TableType
}

type rows struct {
	tbl    *table
	rows   Rows
	curRow []sql.Value
}

func makeTable(tn sql.TableName, st Table, tt *TableType) (*table, sql.TableType, error) {
	return &table{
		tn: tn,
		st: st,
		tt: tt,
	}, tt, nil
}

func (tbl *table) Columns(ctx context.Context) []sql.Identifier {
	return tbl.tt.cols
}

func (tbl *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return tbl.tt.colTypes
}

func (tbl *table) PrimaryKey(ctx context.Context) []sql.ColumnKey {
	return tbl.tt.primary
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

type rowContext []sql.Value

func (rc rowContext) EvalRef(idx int) sql.Value {
	return rc[idx]
}

func (tbl *table) Insert(ctx context.Context, row []sql.Value) error {
	cols := tbl.tt.cols
	for rdx, ct := range tbl.tt.colTypes {
		var err error
		row[rdx], err = convertValue(ct, cols[rdx], row[rdx])
		if err != nil {
			return fmt.Errorf("engine: table %s: %s", tbl.tn, err)
		}
	}

	if len(tbl.tt.checks) > 0 {
		for _, chk := range tbl.tt.checks {
			val, err := chk.check.Eval(ctx, rowContext(row))
			if err != nil {
				return fmt.Errorf("engine: table %s: constraint: %s: %s", tbl.tn, chk.name, err)
			}
			if val != nil {
				if b, ok := val.(sql.BoolValue); ok && b == sql.BoolValue(false) {
					return fmt.Errorf("engine: table %s: check: %s: failed", tbl.tn, chk.name)
				}
			}
		}
	}

	return tbl.st.Insert(ctx, row)
}

func (tbl *table) update(ctx context.Context, r Rows, updates []sql.ColumnUpdate,
	curRow []sql.Value) error {

	cols := tbl.tt.cols
	colTypes := tbl.tt.colTypes
	for _, up := range updates {
		ct := colTypes[up.Column]

		var err error
		up.Value, err = convertValue(ct, cols[up.Column], up.Value)
		if err != nil {
			return fmt.Errorf("engine: table %s: %s", tbl.tn, err)
		}
	}

	updateRow := append(make([]sql.Value, 0, len(curRow)), curRow...)
	for _, update := range updates {
		updateRow[update.Column] = update.Value
	}

	for _, chk := range tbl.tt.checks {
		val, err := chk.check.Eval(ctx, rowContext(updateRow))
		if err != nil {
			return fmt.Errorf("engine: table %s: constraint: %s: %s", tbl.tn, chk.name,
				err)
		}
		if val != nil {
			if b, ok := val.(sql.BoolValue); ok && b == sql.BoolValue(false) {
				return fmt.Errorf("engine: table %s: check: %s: failed", tbl.tn, chk.name)
			}
		}
	}

	return r.Update(ctx, updates, updateRow)
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
	var err error
	r.curRow, err = r.rows.Next(ctx)
	if err != nil {
		r.curRow = nil
		return err
	}
	copy(dest, r.curRow)
	return nil
}

func (r *rows) Delete(ctx context.Context) error {
	return r.rows.Delete(ctx)
}

func (r *rows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	if r.curRow == nil {
		panic(fmt.Sprintf("engine: table %s no row to update", r.tbl.tn))
	}

	return r.tbl.update(ctx, r.rows, updates, r.curRow)
}
