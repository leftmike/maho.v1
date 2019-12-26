package typedtbl_test

import (
	"context"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/engine/typedtbl"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

type testRow struct {
	str        string
	i64        int64
	bytes      []byte
	null_str   *string
	null_f64   *float64
	null_bytes []byte
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
	e, err := basic.NewEngine("testdata")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	dn := sql.ID("typedtbl_test")
	sn := sql.SchemaName{dn, sql.ID("schema")}
	tn := sql.TableName{dn, sn.Schema, sql.ID("table")}

	columns := []sql.Identifier{
		sql.QuotedID("Str"),
		sql.ID("i64"),
		sql.QuotedID("BYTES"),
		sql.QuotedID("NULL_Str"),
		sql.ID("null_f64"),
		sql.ID("null_bytes"),
	}
	columnTypes := []sql.ColumnType{
		sql.StringColType,
		sql.Int64ColType,
		sql.ColumnType{Type: sql.BytesType, Size: sql.MaxColumnSize, NotNull: true},
		sql.NullStringColType,
		sql.ColumnType{Type: sql.FloatType, Size: 8},
		sql.ColumnType{Type: sql.BytesType, Size: sql.MaxColumnSize},
	}
	primaryKey := []engine.ColumnKey{engine.MakeColumnKey(0, false)}

	err = e.CreateDatabase(dn, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}

	tx := e.Begin(0)
	err = e.CreateSchema(ctx, tx, sn)
	if err != nil {
		t.Fatal(err)
	}
	err = e.CreateTable(ctx, tx, tn, columns, columnTypes, primaryKey, false)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	tx = e.Begin(0)
	tbl, err := e.LookupTable(ctx, tx, tn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := tx.Commit(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}()

	ttbl := typedtbl.MakeTable(tn, tbl)

	cols := ttbl.Columns(ctx)
	if !testutil.DeepEqual(cols, columns) {
		t.Errorf("Columns(): got %v want %v", cols, columns)
	}
	colTypes := ttbl.ColumnTypes(ctx)
	if !testutil.DeepEqual(colTypes, columnTypes) {
		t.Errorf("ColumnTypes(): got %v want %v", colTypes, columnTypes)
	}
	// XXX: basic does not yet support primary keys
	//pkey := ttbl.PrimaryKey(ctx)
	//if !testutil.DeepEqual(pkey, primaryKey) {
	//	t.Errorf("PrimaryKey(): got %v want %v", pkey, primaryKey)
	//}

	tr := testRow{
		str:        "string #1",
		i64:        1,
		bytes:      []byte{1, 1},
		null_str:   nil,
		null_f64:   nil,
		null_bytes: nil,
	}
	err = ttbl.Insert(ctx, &tr)
	if err != nil {
		t.Errorf("Insert(%v) failed with %s", tr, err)
	}

	tr2 := testRow{
		str:        "string #2",
		i64:        2,
		bytes:      []byte{2, 2},
		null_str:   typedtbl.NullString("null string #1"),
		null_f64:   typedtbl.NullFloat64(1.1),
		null_bytes: []byte{1, 1},
	}
	err = ttbl.Insert(ctx, tr2)
	if err != nil {
		t.Errorf("Insert(%v) failed with %s", tr, err)
	}

	testPanic(
		func() {
			tr3 := struct {
				i64   int64
				bytes []byte
			}{
				i64:   3,
				bytes: []byte{3, 3},
			}
			ttbl.Insert(ctx, tr3)
		},
		func() {
			t.Errorf("Insert(%v) did not panic", tr)
		})

	testPanic(
		func() {
			tr4 := struct {
				str string
				i64 int64
			}{
				str: "string #4",
				i64: 4,
			}
			ttbl.Insert(ctx, tr4)
		},
		func() {
			t.Errorf("Insert(%v) did not panic", tr)
		})
}
