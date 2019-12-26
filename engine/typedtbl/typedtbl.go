package typedtbl

import (
	"context"
	"errors"
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

type table struct {
	tbl       engine.Table
	tn        sql.TableName
	rowType   reflect.Type
	colFields []columnField
}

type rows struct {
	tbl  *table
	rows engine.Rows
}

func MakeTable(tn sql.TableName, tbl engine.Table) *table {
	return &table{
		tbl: tbl,
		tn:  tn,
	}
}

func (tbl *table) Columns(ctx context.Context) []sql.Identifier {
	return tbl.tbl.Columns(ctx)
}

func (tbl *table) ColumnTypes(ctx context.Context) []sql.ColumnType {
	return tbl.tbl.ColumnTypes(ctx)
}

func (tbl *table) PrimaryKey(ctx context.Context) []engine.ColumnKey {
	return tbl.tbl.PrimaryKey(ctx)
}

func (tbl *table) Scan(ctx context.Context, key interface{}, numKeyCols int) (*rows, error) {
	return nil, errors.New("not implemented") // XXX
}

func (tbl *table) Rows(ctx context.Context) (*rows, error) {
	r, err := tbl.tbl.Rows(ctx)
	if err != nil {
		return nil, err
	}
	return &rows{
		rows: r,
		tbl:  tbl,
	}, nil
}

func (tbl *table) makeColumnField(cn string, ct sql.ColumnType,
	fields map[string]reflect.StructField) columnField {

	sf, ok := fields[strings.ToLower(cn)]
	if !ok {
		if ct.NotNull {
			panic(fmt.Sprintf("typed table: %s: required column %s not found", tbl.tn, cn))
		}
		return columnField{skip: true}
	}

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
		index:    sf.Index[0],
		notNull:  ct.NotNull,
		pointer:  ptr,
	}
}

func (tbl *table) makeColumnFields(ctx context.Context, rowType reflect.Type) []columnField {

	var colFields []columnField

	fields := map[string]reflect.StructField{}
	nf := rowType.NumField()
	for fdx := 0; fdx < nf; fdx++ {
		sf := rowType.Field(fdx)
		fields[strings.ToLower(sf.Name)] = sf
	}

	cols := tbl.tbl.Columns(ctx)
	colTypes := tbl.tbl.ColumnTypes(ctx)
	for cdx := range cols {
		colFields = append(colFields, tbl.makeColumnField(cols[cdx].String(), colTypes[cdx],
			fields))
	}
	return colFields
}

func (tbl *table) Insert(ctx context.Context, rowObj interface{}) error {
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
		tbl.colFields = tbl.makeColumnFields(ctx, rowType)
		tbl.rowType = rowType
	}

	dest := make([]sql.Value, len(tbl.colFields))
	for cdx, cf := range tbl.colFields {
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
		tbl.colFields = tbl.makeColumnFields(ctx, rowType)
		tbl.rowType = rowType
	}

	dest := make([]sql.Value, len(tbl.colFields))
	err := r.rows.Next(ctx, dest)
	if err != nil {
		return err
	}

	for cdx, cf := range tbl.colFields {
		if cf.skip {
			continue
		}

		v := rowVal.Field(cf.index)
		if dest[cdx] == nil {
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

func (r *rows) Update(ctx context.Context, updateObj interface{}) error {
	// Use fields in updateObj to decide what needs changing: all fields are assumed to be updates
	return errors.New("not implemented") // XXX
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
