package evaluate_test

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type testRows struct {
	rdx          int
	closeCalled  bool
	deleteCalled bool
	updateCalled bool
}

func (_ *testRows) NumColumns() int {
	return 3
}

func (tr *testRows) Close() error {
	tr.closeCalled = true
	return nil
}

func (tr *testRows) Next(ctx context.Context, dest []sql.Value) error {
	if len(dest) != 3 {
		return errors.New("len(dest) should be three")
	}
	if tr.rdx < 0 {
		return io.EOF
	}
	dest[0] = sql.Int64Value(tr.rdx)
	dest[1] = sql.Int64Value(tr.rdx * 10)
	dest[2] = sql.Int64Value(tr.rdx * 100)
	tr.rdx -= 1
	return nil
}

func (tr *testRows) Delete(ctx context.Context) error {
	tr.deleteCalled = true
	return nil
}

func (tr *testRows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	tr.updateCalled = true
	return nil
}

func TestAllRows(t *testing.T) {

	tr := &testRows{rdx: 10}
	rows, err := evaluate.AllRows(nil, tr)
	if err != nil {
		t.Errorf("AllRows failed with %s", err)
		return
	}

	if !tr.closeCalled {
		t.Errorf("AllRows did not call Rows.Close; it should")
	}
	if tr.deleteCalled {
		t.Errorf("AllRows called Rows.Delete; it should not")
	}
	if tr.updateCalled {
		t.Errorf("AllRows called Rows.Update; it should not")
	}

	rdx := 10
	for idx, row := range rows {
		if len(row) != 3 {
			t.Errorf("AllRows: len(row[%d]) got %d want 3", idx, len(row))
			continue
		}
		want := []sql.Value{sql.Int64Value(rdx), sql.Int64Value(rdx * 10),
			sql.Int64Value(rdx * 100)}
		if !reflect.DeepEqual(row, want) {
			t.Errorf("AllRows: row[%d] got %v want %v", idx, row, want)
		}
		rdx -= 1
	}
}
