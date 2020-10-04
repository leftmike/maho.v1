package engine_test

import (
	"context"
	"testing"

	"github.com/leftmike/maho/sql"
)

type triggerAdder interface {
	AddTrigger(events int64, trig sql.Trigger)
}

func addTrigger(t *testing.T, tx sql.Transaction, tn sql.TableName, events int64,
	trig sql.Trigger) {

	ctx := context.Background()
	tt, err := tx.LookupTableType(ctx, tn)
	if err != nil {
		t.Fatal(err)
	}
	ta, ok := tt.(triggerAdder)
	if !ok {
		t.Fatalf("AddTrigger: not available on table type: %v", tt)
	}
	ta.AddTrigger(events, trig)
}

type testTrigger struct {
	t         *testing.T
	called    bool
	oldValues [][]sql.Value
	newValues [][]sql.Value
}

func (ttrig *testTrigger) AfterRows(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	ttrig.called = true

	return nil
}

func TestTriggers(t *testing.T) {
	e := startEngine(t, sql.ID("db"))
	tn := sql.TableName{sql.ID("db"), sql.PUBLIC, sql.ID("tbl1")}
	createTable(t, e.Begin(0), tn)

	/*
		ttrig := &testTrigger{
			t:         t,
			newValues: [][]sql.Value{},
		}
		addTrigger(t, e.Begin(0), tn, sql.InsertEvent, ttrig)
	*/
	insertRows(t, e.Begin(0), tn, 1, 8)
	/*
		if !ttrig.called {
			t.Error("AfterRows not called for insert")
		}
	*/

	deleteIndexRow(t, e.Begin(0), tn, 0, strVal("6"))
	deleteIndexRow(t, e.Begin(0), tn, 1, i64Val(2))
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
