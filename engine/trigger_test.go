package engine_test

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

const (
	deleteTriggerType = "deleteTriggerType"
	insertTriggerType = "insertTriggerType"
	updateTriggerType = "updateTriggerType"
)

func addTrigger(t *testing.T, tx sql.Transaction, tn sql.TableName, events int64,
	trig sql.Trigger) {

	ctx := context.Background()
	err := tx.AddTrigger(ctx, tn, events, trig)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

var (
	triggerT         *testing.T
	triggerEvent     int64
	triggerOldValues [][]sql.Value
	triggerNewValues [][]sql.Value
)

func expectTrigger(t *testing.T, oldValues, newValues [][]sql.Value) {
	triggerT = t
	triggerEvent = 0
	triggerOldValues = oldValues
	triggerNewValues = newValues
}

func checkRows(t *testing.T, rows sql.Rows, values [][]sql.Value) {
	if rows == nil {
		if len(values) != 0 {
			t.Errorf("trigger: have values, but no rows: %v", values)
		}
		return
	}

	ctx := context.Background()
	dest := make([]sql.Value, rows.NumColumns())
	for {
		err := rows.Next(ctx, dest)
		if err != nil {
			if err == io.EOF {
				if len(values) != 0 {
					t.Errorf("trigger: have values, but no more rows: %v", values)
				}
			} else {
				t.Errorf("trigger: Rows.Next() failed with %s", err)
			}
			break
		}

		if len(values) == 0 {
			t.Errorf("trigger: have row, but no more values: %v", dest)
			break
		}

		if !reflect.DeepEqual(values[0], dest) {
			t.Errorf("trigger: got %v, want %v", dest, values[0])
		}
		values = values[1:]
	}
}

type deleteTrigger struct{}

func (_ deleteTrigger) Type() string {
	return deleteTriggerType
}

func (_ deleteTrigger) Encode() ([]byte, error) {
	return []byte{0}, nil
}

func decodeDeleteTrigger(buf []byte) (sql.Trigger, error) {
	if len(buf) != 1 || buf[0] != 0 {
		return nil, fmt.Errorf("unable to decode test trigger: %v", buf)
	}
	return deleteTrigger{}, nil
}

func (_ deleteTrigger) AfterRows(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	triggerEvent = sql.DeleteEvent
	checkRows(triggerT, oldRows, triggerOldValues)
	checkRows(triggerT, newRows, triggerNewValues)
	return nil
}

type insertTrigger struct{}

func (_ insertTrigger) Type() string {
	return insertTriggerType
}

func (_ insertTrigger) Encode() ([]byte, error) {
	return []byte{0}, nil
}

func decodeInsertTrigger(buf []byte) (sql.Trigger, error) {
	if len(buf) != 1 || buf[0] != 0 {
		return nil, fmt.Errorf("unable to decode test trigger: %v", buf)
	}
	return insertTrigger{}, nil
}

func (_ insertTrigger) AfterRows(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	triggerEvent = sql.InsertEvent
	checkRows(triggerT, oldRows, triggerOldValues)
	checkRows(triggerT, newRows, triggerNewValues)
	return nil
}

type updateTrigger struct{}

func (_ updateTrigger) Type() string {
	return updateTriggerType
}

func (_ updateTrigger) Encode() ([]byte, error) {
	return []byte{0}, nil
}

func decodeUpdateTrigger(buf []byte) (sql.Trigger, error) {
	if len(buf) != 1 || buf[0] != 0 {
		return nil, fmt.Errorf("unable to decode test trigger: %v", buf)
	}
	return updateTrigger{}, nil
}

func (_ updateTrigger) AfterRows(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	triggerEvent = sql.UpdateEvent
	checkRows(triggerT, oldRows, triggerOldValues)
	checkRows(triggerT, newRows, triggerNewValues)
	return nil
}

func TestTriggers(t *testing.T) {
	e := startEngine(t, sql.ID("db"))
	tn := sql.TableName{sql.ID("db"), sql.PUBLIC, sql.ID("tbl1")}
	createTable(t, e.Begin(0), tn)
	addTrigger(t, e.Begin(0), tn, sql.DeleteEvent, deleteTrigger{})
	addTrigger(t, e.Begin(0), tn, sql.InsertEvent, insertTrigger{})
	addTrigger(t, e.Begin(0), tn, sql.UpdateEvent, updateTrigger{})

	expectTrigger(t, nil, [][]sql.Value{
		{i64Val(1), strVal("8"), i64Val(0), i64Val(0)},
		{i64Val(2), strVal("7"), i64Val(0), i64Val(1)},
		{i64Val(3), strVal("6"), i64Val(1), i64Val(2)},
		{i64Val(4), strVal("5"), i64Val(1), i64Val(3)},
		{i64Val(5), strVal("4"), i64Val(2), i64Val(4)},
		{i64Val(6), strVal("3"), i64Val(2), i64Val(5)},
		{i64Val(7), strVal("2"), i64Val(3), i64Val(6)},
		{i64Val(8), strVal("1"), i64Val(3), i64Val(7)},
	})
	insertRows(t, e.Begin(0), tn, 1, 8)
	if triggerEvent != sql.InsertEvent {
		t.Error("AfterRows not called for insert")
	}

	expectTrigger(t, [][]sql.Value{
		{i64Val(3), strVal("6"), i64Val(1), i64Val(2)},
	}, nil)
	deleteIndexRow(t, e.Begin(0), tn, 0, strVal("6"))
	if triggerEvent != sql.DeleteEvent {
		t.Error("AfterRows not called for delete")
	}

	expectTrigger(t, [][]sql.Value{
		{i64Val(5), strVal("4"), i64Val(2), i64Val(4)},
	}, nil)
	deleteRow(t, e.Begin(0), tn, i64Val(5))
	if triggerEvent != sql.DeleteEvent {
		t.Error("AfterRows not called for delete")
	}

	expectTrigger(t, [][]sql.Value{
		{i64Val(4), strVal("5"), i64Val(1), i64Val(3)},
	}, [][]sql.Value{
		{i64Val(4), strVal("5"), i64Val(1), i64Val(30)},
	})
	updateIndexRow(t, e.Begin(0), tn, 0, strVal("5"), []sql.ColumnUpdate{{3, i64Val(30)}})
	if triggerEvent != sql.UpdateEvent {
		t.Error("AfterRows not called for update")
	}

	expectTrigger(t, [][]sql.Value{
		{i64Val(6), strVal("3"), i64Val(2), i64Val(5)},
	}, [][]sql.Value{
		{i64Val(6), strVal("6"), i64Val(2), i64Val(5)},
	})
	updateRow(t, e.Begin(0), tn, i64Val(6), []sql.ColumnUpdate{{1, strVal("6")}})
	if triggerEvent != sql.UpdateEvent {
		t.Error("AfterRows not called for update")
	}

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

func init() {
	engine.TriggerDecoders[deleteTriggerType] = decodeDeleteTrigger
	engine.TriggerDecoders[insertTriggerType] = decodeInsertTrigger
	engine.TriggerDecoders[updateTriggerType] = decodeUpdateTrigger
}

func addForeignKey(t *testing.T, tx sql.Transaction, con sql.Identifier, fktn sql.TableName,
	fkCols []int, rtn sql.TableName, ridx sql.Identifier) {

	ctx := context.Background()
	err := tx.AddForeignKey(ctx, con, fktn, fkCols, rtn, ridx, sql.NoAction, sql.NoAction)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func createIndex(t *testing.T, tx sql.Transaction, idxname sql.Identifier, tn sql.TableName,
	unique bool, key []sql.ColumnKey) {

	ctx := context.Background()
	err := tx.CreateIndex(ctx, idxname, tn, unique, key, false)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestForeignKeys(t *testing.T) {
	e := startEngine(t, sql.ID("db"))
	tn1 := sql.TableName{sql.ID("db"), sql.PUBLIC, sql.ID("tbl1")}
	tn2 := sql.TableName{sql.ID("db"), sql.PUBLIC, sql.ID("tbl2")}
	createTable(t, e.Begin(0), tn1)
	createTable(t, e.Begin(0), tn2)
	addForeignKey(t, e.Begin(0), sql.ID("fk1"), tn1, []int{2}, tn2, sql.PRIMARY_QUOTED)

	idx1 := sql.ID("idx1")
	createIndex(t, e.Begin(0), idx1, tn1, true,
		[]sql.ColumnKey{sql.MakeColumnKey(3, false), sql.MakeColumnKey(2, false)})
	addForeignKey(t, e.Begin(0), sql.ID("fk2"), tn2, []int{4, 1}, tn1, idx1)
}
