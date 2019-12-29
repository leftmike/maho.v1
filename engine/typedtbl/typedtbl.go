package typedtbl

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type columnField struct {
	dataType sql.DataType
	index    int
	skip     bool
	notNull  bool
	pointer  bool
}

type Table struct {
	tbl          engine.Table
	tn           sql.TableName
	rowType      reflect.Type
	rowFields    []columnField
	updateType   reflect.Type
	updateFields []columnField
}

type rows struct {
	tbl  *Table
	rows engine.Rows
}

func MakeTable(tn sql.TableName, tbl engine.Table) *Table {
	return &Table{
		tbl: tbl,
		tn:  tn,
	}
}

func (tbl *Table) Columns(ctx context.Context) []sql.Identifier {
	return tbl.tbl.Columns(ctx)
}

func (tbl *Table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return tbl.tbl.ColumnTypes(ctx)
}

func (tbl *Table) PrimaryKey(ctx context.Context) []engine.ColumnKey {
	return tbl.tbl.PrimaryKey(ctx)
}

func (tbl *Table) Scan(ctx context.Context, key []sql.Value, numKeyCols int) (*rows, error) {
	r, err := tbl.tbl.Scan(ctx, key, numKeyCols)
	if err != nil {
		return nil, err
	}
	return &rows{
		rows: r,
		tbl:  tbl,
	}, nil
}

func (tbl *Table) Rows(ctx context.Context) (*rows, error) {
	r, err := tbl.tbl.Rows(ctx)
	if err != nil {
		return nil, err
	}
	return &rows{
		rows: r,
		tbl:  tbl,
	}, nil
}

func (tbl *Table) makeColumnField(cn string, ct sql.ColumnType, idx int,
	sf reflect.StructField) columnField {

	ptr := !ct.NotNull
	switch ct.Type {
	case sql.BooleanType:
		if ct.NotNull {
			if sf.Type.Kind() != reflect.Bool {
				panic(fmt.Sprintf("typed table: %s: column %s is bool; struct field %s is %s",
					tbl.tn, cn, sf.Name, sf.Type.String()))
			}
		} else {
			if sf.Type.Kind() != reflect.Ptr || sf.Type.Elem().Kind() != reflect.Bool {
				panic(fmt.Sprintf("typed table: %s: column %s is *bool; struct field %s is %s",
					tbl.tn, cn, sf.Name, sf.Type.String()))
			}
		}
	case sql.StringType:
		if ct.NotNull {
			if sf.Type.Kind() != reflect.String {
				panic(fmt.Sprintf("typed table: %s: column %s is string; struct field %s is %s",
					tbl.tn, cn, sf.Name, sf.Type.String()))
			}
		} else {
			if sf.Type.Kind() != reflect.Ptr || sf.Type.Elem().Kind() != reflect.String {
				panic(fmt.Sprintf("typed table: %s: column %s is *string; struct field %s is %s",
					tbl.tn, cn, sf.Name, sf.Type.String()))
			}
		}
	case sql.BytesType:
		if sf.Type.Kind() != reflect.Slice || sf.Type.Elem().Kind() != reflect.Uint8 {
			panic(fmt.Sprintf("typed table: %s: column %s is []byte; struct field %s is %s",
				tbl.tn, cn, sf.Name, sf.Type.String()))
		}
		ptr = false // Bytes is never a pointer.
	case sql.FloatType:
		if ct.NotNull {
			if sf.Type.Kind() != reflect.Float64 {
				panic(fmt.Sprintf("typed table: %s: column %s is float64; struct field %s is %s",
					tbl.tn, cn, sf.Name, sf.Type.String()))
			}
		} else {
			if sf.Type.Kind() != reflect.Ptr || sf.Type.Elem().Kind() != reflect.Float64 {
				panic(fmt.Sprintf("typed table: %s: column %s is *float64; struct field %s is %s",
					tbl.tn, cn, sf.Name, sf.Type.String()))
			}
		}
	case sql.IntegerType:
		if ct.NotNull {
			if sf.Type.Kind() != reflect.Int64 {
				panic(fmt.Sprintf("typed table: %s: column %s is int64; struct field %s is %s",
					tbl.tn, cn, sf.Name, sf.Type.String()))
			}
		} else {
			if sf.Type.Kind() != reflect.Ptr || sf.Type.Elem().Kind() != reflect.Int64 {
				panic(fmt.Sprintf("typed table: %s: column %s is *int64; struct field %s is %s",
					tbl.tn, cn, sf.Name, sf.Type.String()))
			}
		}
	}

	return columnField{
		dataType: ct.Type,
		index:    idx,
		notNull:  ct.NotNull,
		pointer:  ptr,
	}
}

func (tbl *Table) makeRowFields(ctx context.Context, rowType reflect.Type) []columnField {
	var rowFields []columnField

	fields := map[string]reflect.StructField{}
	nf := rowType.NumField()
	for fdx := 0; fdx < nf; fdx++ {
		sf := rowType.Field(fdx)
		fields[strings.ToLower(sf.Name)] = sf
	}

	cols := tbl.tbl.Columns(ctx)
	colTypes := tbl.tbl.ColumnTypes(ctx)
	for cdx := range cols {
		cn := cols[cdx].String()
		ct := colTypes[cdx]
		sf, ok := fields[strings.ToLower(cn)]
		if !ok {
			if ct.NotNull {
				panic(fmt.Sprintf("typed table: %s: required column %s not found", tbl.tn, cn))
			}
			rowFields = append(rowFields, columnField{skip: true})
		} else {
			rowFields = append(rowFields, tbl.makeColumnField(cn, ct, sf.Index[0], sf))
		}
	}
	return rowFields
}

func (tbl *Table) Insert(ctx context.Context, rowObj interface{}) error {
	rowType := reflect.TypeOf(rowObj)
	rowVal := reflect.ValueOf(rowObj)
	if rowType.Kind() == reflect.Ptr {
		rowType = rowType.Elem()
		rowVal = rowVal.Elem()
	}
	if rowType.Kind() != reflect.Struct {
		panic(fmt.Sprintf("typed table: rowObj must be a struct or a pointer to a struct; got %v",
			rowObj))
	}
	if rowType != tbl.rowType {
		tbl.rowFields = tbl.makeRowFields(ctx, rowType)
		tbl.rowType = rowType
	}

	dest := make([]sql.Value, len(tbl.rowFields))
	for cdx, cf := range tbl.rowFields {
		if cf.skip {
			continue
		}

		v := rowVal.Field(cf.index)
		if cf.pointer {
			if v.IsNil() {
				continue
			}
			v = v.Elem()
		}

		switch cf.dataType {
		case sql.BooleanType:
			dest[cdx] = sql.BoolValue(v.Bool())
		case sql.StringType:
			dest[cdx] = sql.StringValue(v.String())
		case sql.BytesType:
			dest[cdx] = sql.BytesValue(v.Bytes())
		case sql.FloatType:
			dest[cdx] = sql.Float64Value(v.Float())
		case sql.IntegerType:
			dest[cdx] = sql.Int64Value(v.Int())
		}
	}

	return tbl.tbl.Insert(ctx, dest)
}

func (r *rows) Columns() []sql.Identifier {
	return r.rows.Columns()
}

func (r *rows) Close() error {
	err := r.rows.Close()
	r.rows = nil
	return err
}

func (r *rows) Next(ctx context.Context, destObj interface{}) error {
	tbl := r.tbl

	rowType := reflect.TypeOf(destObj)
	rowVal := reflect.ValueOf(destObj)
	if rowType.Kind() != reflect.Ptr || rowType.Elem().Kind() != reflect.Struct {
		panic(fmt.Sprintf("typed table: destObj must be a pointer to a struct; got %v", destObj))
	}
	rowType = rowType.Elem()
	rowVal = rowVal.Elem()

	if rowType != tbl.rowType {
		tbl.rowFields = tbl.makeRowFields(ctx, rowType)
		tbl.rowType = rowType
	}

	dest := make([]sql.Value, len(tbl.rowFields))
	err := r.rows.Next(ctx, dest)
	if err != nil {
		return err
	}

	for cdx, cf := range tbl.rowFields {
		if cf.skip {
			continue
		}

		v := rowVal.Field(cf.index)
		if dest[cdx] == nil {
			v.Set(reflect.Zero(v.Type()))
			continue
		}

		switch cf.dataType {
		case sql.BooleanType:
			bv, ok := dest[cdx].(sql.BoolValue)
			if ok {
				b := bool(bv)
				if cf.pointer {
					v.Set(reflect.ValueOf(&b))
				} else {
					v.SetBool(b)
				}
			}
		case sql.StringType:
			sv, ok := dest[cdx].(sql.StringValue)
			if ok {
				s := string(sv)
				if cf.pointer {
					v.Set(reflect.ValueOf(&s))
				} else {
					v.SetString(s)
				}
			}
		case sql.BytesType:
			b, ok := dest[cdx].(sql.BytesValue)
			if ok {
				v.SetBytes([]byte(b))
			}
		case sql.FloatType:
			fv, ok := dest[cdx].(sql.Float64Value)
			if ok {
				f := float64(fv)
				if cf.pointer {
					v.Set(reflect.ValueOf(&f))
				} else {
					v.SetFloat(f)
				}
			}
		case sql.IntegerType:
			iv, ok := dest[cdx].(sql.Int64Value)
			if ok {
				i := int64(iv)
				if cf.pointer {
					v.Set(reflect.ValueOf(&i))
				} else {
					v.SetInt(i)
				}
			}
		}
	}
	return nil
}

func (r *rows) Delete(ctx context.Context) error {
	return r.rows.Delete(ctx)
}

func (tbl *Table) makeUpdateFields(ctx context.Context, updateType reflect.Type) []columnField {
	var updateFields []columnField

	type column struct {
		cn  string
		ct  sql.ColumnType
		idx int
	}
	fields := map[string]column{}

	cols := tbl.tbl.Columns(ctx)
	colTypes := tbl.tbl.ColumnTypes(ctx)
	for cdx := range cols {
		fields[strings.ToLower(cols[cdx].String())] = column{cols[cdx].String(), colTypes[cdx], cdx}
	}

	nf := updateType.NumField()
	for fdx := 0; fdx < nf; fdx++ {
		sf := updateType.Field(fdx)
		col, ok := fields[strings.ToLower(sf.Name)]
		if !ok {
			panic(fmt.Sprintf("typed table: %s: field %s not found", tbl.tn, sf.Name))
		}
		updateFields = append(updateFields, tbl.makeColumnField(col.cn, col.ct, col.idx, sf))
	}
	return updateFields
}

func (r *rows) Update(ctx context.Context, updateObj interface{}) error {
	tbl := r.tbl

	updateType := reflect.TypeOf(updateObj)
	updateVal := reflect.ValueOf(updateObj)
	if updateType.Kind() == reflect.Ptr {
		updateType = updateType.Elem()
		updateVal = updateVal.Elem()
	}
	if updateType.Kind() != reflect.Struct {
		panic(
			fmt.Sprintf("typed table: updateObj must be a struct or a pointer to a struct; got %v",
				updateObj))
	}
	if updateType != tbl.updateType {
		tbl.updateFields = tbl.makeUpdateFields(ctx, updateType)
		tbl.updateType = updateType
	}

	updates := make([]sql.ColumnUpdate, len(tbl.updateFields))
	for fdx, cf := range tbl.updateFields {
		updates[fdx].Index = cf.index

		v := updateVal.Field(fdx)
		if cf.pointer {
			if v.IsNil() {
				continue
			}
			v = v.Elem()
		}

		switch cf.dataType {
		case sql.BooleanType:
			updates[fdx].Value = sql.BoolValue(v.Bool())
		case sql.StringType:
			updates[fdx].Value = sql.StringValue(v.String())
		case sql.BytesType:
			updates[fdx].Value = sql.BytesValue(v.Bytes())
		case sql.FloatType:
			updates[fdx].Value = sql.Float64Value(v.Float())
		case sql.IntegerType:
			updates[fdx].Value = sql.Int64Value(v.Int())
		}
	}

	return r.rows.Update(ctx, updates)
}

func NullBoolean(b bool) *bool {
	return &b
}

func NullString(s string) *string {
	return &s
}

func NullFloat64(f64 float64) *float64 {
	return &f64
}

func NullInt64(i64 int64) *int64 {
	return &i64
}
