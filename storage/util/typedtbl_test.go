package util_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage/basic"
	"github.com/leftmike/maho/storage/util"
	"github.com/leftmike/maho/testutil"
)

type testRow struct {
	Str       string
	I64       int64
	Bytes     []byte
	NullStr   *string
	NullF64   *float64
	NullBytes []byte
}

func testPanic(test func(), noPanic func()) {
	defer func() {
		if r := recover(); r == nil {
			noPanic()
		}
	}()

	test()
}

func TestTypedTable(t *testing.T) {
	st, err := basic.NewStore("testdata")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	dn := sql.ID("typedtbl_test")
	sn := sql.SchemaName{dn, sql.ID("schema")}
	tn := sql.TableName{dn, sn.Schema, sql.ID("table")}

	columns := []sql.Identifier{
		sql.QuotedID("str"),
		sql.ID("i64"),
		sql.QuotedID("BYTES"),
		sql.QuotedID("NULLStr"),
		sql.ID("nullf64"),
		sql.ID("nullbytes"),
	}
	columnTypes := []sql.ColumnType{
		sql.StringColType,
		sql.Int64ColType,
		sql.ColumnType{Type: sql.BytesType, Size: sql.MaxColumnSize, NotNull: true},
		sql.NullStringColType,
		sql.ColumnType{Type: sql.FloatType, Size: 8},
		sql.ColumnType{Type: sql.BytesType, Size: sql.MaxColumnSize},
	}
	primaryKey := []sql.ColumnKey{sql.MakeColumnKey(0, false)}

	err = st.CreateDatabase(dn, map[sql.Identifier]string{})
	if err != nil {
		t.Fatal(err)
	}

	tx := st.Begin(0)
	err = st.CreateSchema(ctx, tx, sn)
	if err != nil {
		t.Fatal(err)
	}
	err = st.CreateTable(ctx, tx, tn, columns, columnTypes, primaryKey, false)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	tx = st.Begin(0)
	tbl, err := st.LookupTable(ctx, tx, tn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := tx.Commit(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}()

	ttbl := util.MakeTypedTable(tn, tbl)

	cols := ttbl.Columns(ctx)
	if !testutil.DeepEqual(cols, columns) {
		t.Errorf("Columns(): got %v want %v", cols, columns)
	}
	colTypes := ttbl.ColumnTypes(ctx)
	if !testutil.DeepEqual(colTypes, columnTypes) {
		t.Errorf("ColumnTypes(): got %v want %v", colTypes, columnTypes)
	}
	pkey := ttbl.PrimaryKey(ctx)
	if !testutil.DeepEqual(pkey, primaryKey) {
		t.Errorf("PrimaryKey(): got %v want %v", pkey, primaryKey)
	}

	tr := []testRow{
		{
			Str:       "string #1",
			I64:       1,
			Bytes:     []byte{1, 1},
			NullStr:   nil,
			NullF64:   nil,
			NullBytes: nil,
		},
		{
			Str:       "string #2",
			I64:       2,
			Bytes:     []byte{2, 2},
			NullStr:   util.NullString("null string #1"),
			NullF64:   util.NullFloat64(1.1),
			NullBytes: []byte{1, 1},
		},
		{
			Str:   "string #3",
			I64:   3,
			Bytes: []byte{3, 3, 3},
		},
		{
			Str:       "string #4",
			I64:       4,
			Bytes:     []byte{4, 4, 4, 4},
			NullStr:   util.NullString(""),
			NullF64:   util.NullFloat64(0.0),
			NullBytes: []byte{},
		},
	}
	err = ttbl.Insert(ctx, &tr[0])
	if err != nil {
		t.Errorf("Insert(%v) failed with %s", tr, err)
	}

	for rdx := 1; rdx < len(tr); rdx += 1 {
		err = ttbl.Insert(ctx, tr[rdx])
		if err != nil {
			t.Errorf("Insert(%v) failed with %s", tr[rdx], err)
		}
	}

	testPanic(
		func() {
			tr := struct {
				I64   int64
				Bytes []byte
			}{
				I64:   3,
				Bytes: []byte{3, 3},
			}
			ttbl.Insert(ctx, tr)
		},
		func() {
			t.Errorf("Insert(%v) did not panic", tr)
		})

	testPanic(
		func() {
			tr := struct {
				Str string
				I64 int64
			}{
				Str: "string #4",
				I64: 4,
			}
			ttbl.Insert(ctx, tr)
		},
		func() {
			t.Errorf("Insert(%v) did not panic", tr)
		})

	r, err := ttbl.Rows(ctx, nil, nil)
	if err != nil {
		t.Errorf("Rows() failed with %s", err)
	}
	var dest testRow
	for rdx := 0; ; rdx += 1 {
		err = r.Next(ctx, &dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("Next() failed with %s", err)
		}
		if !testutil.DeepEqual(dest, tr[rdx]) {
			t.Errorf("Next(%d): got %v want %v", rdx, dest, tr[rdx])
		}
	}
	err = r.Close()
	if err != nil {
		t.Errorf("Close() failed with %s", err)
	}

	r, err = ttbl.Rows(ctx, nil, nil)
	if err != nil {
		t.Errorf("Rows() failed with %s", err)
	}
	for {
		err = r.Next(ctx, &dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("Next() failed with %s", err)
		}
		var s string
		if dest.NullStr == nil {
			s = fmt.Sprintf("<nil> at %d", dest.I64)
		} else {
			s = *dest.NullStr
		}
		err = r.Update(ctx, struct {
			Str       string
			NullF64   *float64
			NullBytes []byte
		}{
			Str:       s,
			NullF64:   util.NullFloat64(float64(dest.I64) * 12.34),
			NullBytes: append(dest.NullBytes, dest.NullBytes...),
		})
		if err != nil {
			t.Errorf("Update() failed with %s", err)
		}
	}
	err = r.Close()
	if err != nil {
		t.Errorf("Close() failed with %s", err)
	}

}
