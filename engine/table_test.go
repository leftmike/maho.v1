package engine_test

import (
	"context"
	"io"
	"reflect"
	"strconv"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage/basic"
)

func i64Val(i int) sql.Value    { return sql.Int64Value(i) }
func strVal(s string) sql.Value { return sql.StringValue(s) }

func startEngine(t *testing.T, db sql.Identifier) sql.Engine {
	st, err := basic.NewStore("basic")
	if err != nil {
		t.Fatal(err)
	}
	e := engine.NewEngine(st)
	err = e.CreateDatabase(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func createTable(t *testing.T, tx sql.Transaction, tn sql.TableName) {
	ctx := context.Background()

	err := tx.CreateTable(ctx, tn,
		[]sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
		[]sql.ColumnType{
			{Type: sql.IntegerType, Size: 8, NotNull: true},
			{Type: sql.StringType, Size: 16, NotNull: true},
			{Type: sql.IntegerType, Size: 8},
			{Type: sql.IntegerType, Size: 8},
		},
		[]sql.Constraint{
			{
				Type: sql.PrimaryConstraint,
				Name: sql.ID("c1-primary"),
				Key:  []sql.ColumnKey{sql.MakeColumnKey(0, false)},
			},
			{
				Type: sql.UniqueConstraint,
				Name: sql.ID("c2-unique"),
				Key:  []sql.ColumnKey{sql.MakeColumnKey(1, false)},
			},
		},
		false)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.NextStmt(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.CreateIndex(ctx, sql.ID("c3-index"), tn, false,
		[]sql.ColumnKey{sql.MakeColumnKey(2, false)}, false)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func lookupTable(t *testing.T, tx sql.Transaction, tn sql.TableName) sql.Table {
	ctx := context.Background()
	tt, err := tx.LookupTableType(ctx, tn)
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := tx.LookupTable(ctx, tn, tt.Version())
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func insertRows(t *testing.T, tx sql.Transaction, tn sql.TableName, min, max int) {
	ctx := context.Background()
	tbl := lookupTable(t, tx, tn)

	err := tbl.ModifyStart(ctx, sql.InsertEvent)
	if err != nil {
		t.Fatal(err)
	}

	cnt := 0
	for min+cnt <= max {
		err = tbl.Insert(ctx,
			[]sql.Value{
				i64Val(cnt + min),
				strVal(strconv.FormatInt(int64(max-cnt), 10)),
				i64Val(cnt / 2),
				i64Val(cnt),
			})
		if err != nil {
			t.Fatal(err)
		}

		cnt += 1
	}

	n, err := tbl.ModifyDone(ctx, sql.InsertEvent, int64(cnt))
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(cnt) {
		t.Errorf("ModifyDone: got %d, want %d", n, cnt)
	}

	err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func indexRows(t *testing.T, tx sql.Transaction, tn sql.TableName, iidx int,
	vals, rows [][]sql.Value) {

	ctx := context.Background()
	tbl := lookupTable(t, tx, tn)

	ir, err := tbl.IndexRows(ctx, iidx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	dest := make([]sql.Value, len(vals[0]))
	row := make([]sql.Value, len(rows[0]))
	for {
		err = ir.Next(ctx, dest)
		if err == io.EOF {
			if len(vals) != 0 {
				t.Errorf("IndexRows: not enough results: %v", vals)
			}
			break
		} else if err != nil {
			t.Errorf("IndexRows.Next: failed with %s", err)
		}

		if len(vals) == 0 {
			t.Errorf("IndexRows: not enough values: %v", dest)
			break
		}
		if !reflect.DeepEqual(dest, vals[0]) {
			t.Errorf("IndexRows: got %v, want %v", dest, vals[0])
		}
		vals = vals[1:]

		err = ir.Row(ctx, row)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(row, rows[0]) {
			t.Errorf("IndexRows.Row(): got %v, want %v", row, rows[0])
		}
		rows = rows[1:]
	}

	err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func deleteIndexRow(t *testing.T, tx sql.Transaction, tn sql.TableName, iidx int, val sql.Value) {
	ctx := context.Background()
	tbl := lookupTable(t, tx, tn)

	ir, err := tbl.IndexRows(ctx, iidx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = tbl.ModifyStart(ctx, sql.DeleteEvent)
	if err != nil {
		t.Fatal(err)
	}

	dest := make([]sql.Value, 4)
	for {
		err = ir.Next(ctx, dest)
		if err != nil {
			t.Errorf("IndexRows.Next: failed with %s", err)
			break
		}
		if reflect.DeepEqual(dest[0], val) {
			err = ir.Delete(ctx)
			if err != nil {
				t.Errorf("IndexRows.Delete: failed with %s", err)
			}
			break
		}
	}

	n, err := tbl.ModifyDone(ctx, sql.DeleteEvent, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("ModifyDone: got %d, want 1", n)
	}

	err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func updateIndexRow(t *testing.T, tx sql.Transaction, tn sql.TableName, iidx int, val sql.Value,
	updates []sql.ColumnUpdate) {

	ctx := context.Background()
	tbl := lookupTable(t, tx, tn)

	ir, err := tbl.IndexRows(ctx, iidx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = tbl.ModifyStart(ctx, sql.UpdateEvent)
	if err != nil {
		t.Fatal(err)
	}

	dest := make([]sql.Value, 4)
	for {
		err = ir.Next(ctx, dest)
		if err != nil {
			t.Errorf("IndexRows.Next: failed with %s", err)
			break
		}
		if reflect.DeepEqual(dest[0], val) {
			err = ir.Update(ctx, updates)
			if err != nil {
				t.Errorf("IndexRows.Update: failed with %s", err)
			}
			break
		}
	}

	n, err := tbl.ModifyDone(ctx, sql.UpdateEvent, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("ModifyDone: got %d, want 1", n)
	}

	err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexRows(t *testing.T) {
	e := startEngine(t, sql.ID("db"))
	tn := sql.TableName{sql.ID("db"), sql.PUBLIC, sql.ID("tbl1")}
	createTable(t, e.Begin(0), tn)
	insertRows(t, e.Begin(0), tn, 1, 8)

	indexRows(t, e.Begin(0), tn, 0,
		[][]sql.Value{
			{strVal("1"), i64Val(8)},
			{strVal("2"), i64Val(7)},
			{strVal("3"), i64Val(6)},
			{strVal("4"), i64Val(5)},
			{strVal("5"), i64Val(4)},
			{strVal("6"), i64Val(3)},
			{strVal("7"), i64Val(2)},
			{strVal("8"), i64Val(1)},
		},
		[][]sql.Value{
			{i64Val(8), strVal("1"), i64Val(3), i64Val(7)},
			{i64Val(7), strVal("2"), i64Val(3), i64Val(6)},
			{i64Val(6), strVal("3"), i64Val(2), i64Val(5)},
			{i64Val(5), strVal("4"), i64Val(2), i64Val(4)},
			{i64Val(4), strVal("5"), i64Val(1), i64Val(3)},
			{i64Val(3), strVal("6"), i64Val(1), i64Val(2)},
			{i64Val(2), strVal("7"), i64Val(0), i64Val(1)},
			{i64Val(1), strVal("8"), i64Val(0), i64Val(0)},
		})

	indexRows(t, e.Begin(0), tn, 1,
		[][]sql.Value{
			{i64Val(0), i64Val(1)},
			{i64Val(0), i64Val(2)},
			{i64Val(1), i64Val(3)},
			{i64Val(1), i64Val(4)},
			{i64Val(2), i64Val(5)},
			{i64Val(2), i64Val(6)},
			{i64Val(3), i64Val(7)},
			{i64Val(3), i64Val(8)},
		},
		[][]sql.Value{
			{i64Val(1), strVal("8"), i64Val(0), i64Val(0)},
			{i64Val(2), strVal("7"), i64Val(0), i64Val(1)},
			{i64Val(3), strVal("6"), i64Val(1), i64Val(2)},
			{i64Val(4), strVal("5"), i64Val(1), i64Val(3)},
			{i64Val(5), strVal("4"), i64Val(2), i64Val(4)},
			{i64Val(6), strVal("3"), i64Val(2), i64Val(5)},
			{i64Val(7), strVal("2"), i64Val(3), i64Val(6)},
			{i64Val(8), strVal("1"), i64Val(3), i64Val(7)},
		})

	deleteIndexRow(t, e.Begin(0), tn, 0, strVal("6"))
	deleteIndexRow(t, e.Begin(0), tn, 1, i64Val(2))

	indexRows(t, e.Begin(0), tn, 1,
		[][]sql.Value{
			{i64Val(0), i64Val(1)},
			{i64Val(0), i64Val(2)},
			{i64Val(1), i64Val(4)},
			{i64Val(2), i64Val(6)},
			{i64Val(3), i64Val(7)},
			{i64Val(3), i64Val(8)},
		},
		[][]sql.Value{
			{i64Val(1), strVal("8"), i64Val(0), i64Val(0)},
			{i64Val(2), strVal("7"), i64Val(0), i64Val(1)},
			{i64Val(4), strVal("5"), i64Val(1), i64Val(3)},
			{i64Val(6), strVal("3"), i64Val(2), i64Val(5)},
			{i64Val(7), strVal("2"), i64Val(3), i64Val(6)},
			{i64Val(8), strVal("1"), i64Val(3), i64Val(7)},
		})

	updateIndexRow(t, e.Begin(0), tn, 0, strVal("5"), []sql.ColumnUpdate{{3, i64Val(30)}})
	updateIndexRow(t, e.Begin(0), tn, 1, i64Val(2), []sql.ColumnUpdate{{1, strVal("6")}})

	indexRows(t, e.Begin(0), tn, 0,
		[][]sql.Value{
			{strVal("1"), i64Val(8)},
			{strVal("2"), i64Val(7)},
			{strVal("5"), i64Val(4)},
			{strVal("6"), i64Val(6)},
			{strVal("7"), i64Val(2)},
			{strVal("8"), i64Val(1)},
		},
		[][]sql.Value{
			{i64Val(8), strVal("1"), i64Val(3), i64Val(7)},
			{i64Val(7), strVal("2"), i64Val(3), i64Val(6)},
			{i64Val(4), strVal("5"), i64Val(1), i64Val(30)},
			{i64Val(6), strVal("6"), i64Val(2), i64Val(5)},
			{i64Val(2), strVal("7"), i64Val(0), i64Val(1)},
			{i64Val(1), strVal("8"), i64Val(0), i64Val(0)},
		})
}
