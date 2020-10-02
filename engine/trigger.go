package engine

import (
	"context"
	"io"

	"github.com/leftmike/maho/sql"
)

type Trigger interface {
	Encode() ([]byte, error)
	AfterRows(ctx context.Context, e *Engine, tx *transaction, oldRows, newRows Rows) error
}

type trigger struct {
	typ    string
	name   sql.Identifier
	events int64
	trig   Trigger
}

type decodeTrigger func(buf []byte) (Trigger, error)

var (
	triggerDecoders = map[string]decodeTrigger{}
)

type triggerRows struct {
	numCols int
	vals    [][]sql.Value
}

func (tr *triggerRows) NumColumns() int {
	return tr.numCols
}

func (tr *triggerRows) Close() error {
	tr.vals = nil
	return nil
}

func (tr *triggerRows) Next(ctx context.Context) ([]sql.Value, error) {
	if len(tr.vals) == 0 {
		return nil, io.EOF
	}
	row := tr.vals[0]
	tr.vals = tr.vals[1:]
	return row, nil
}

func (tr *triggerRows) Delete(ctx context.Context) error {
	panic("engine: trigger rows may not be deleted")
}

func (tr *triggerRows) Update(ctx context.Context, updatedCols []int,
	updateRow []sql.Value) error {

	panic("engine: trigger rows may not be updated")
}

func (tbl *table) ModifyStart(ctx context.Context, event int64) error {
	tbl.deletedRows = nil
	tbl.insertedRows = nil
	tbl.updatedOldRows = nil
	tbl.updatedNewRows = nil
	return nil
}

func (tbl *table) ModifyDone(ctx context.Context, event, cnt int64) (int64, error) {
	for _, t := range tbl.tt.triggers {
		if t.events&sql.DeleteEvent != 0 && tbl.deletedRows != nil {
			oldRows := &triggerRows{
				numCols: len(tbl.tt.Columns()),
				vals:    tbl.deletedRows,
			}
			err := t.trig.AfterRows(ctx, tbl.tx.e, tbl.tx, oldRows, nil)
			if err != nil {
				return -1, err
			}
		}

		if t.events&sql.InsertEvent != 0 && tbl.insertedRows != nil {
			newRows := &triggerRows{
				numCols: len(tbl.tt.Columns()),
				vals:    tbl.insertedRows,
			}
			err := t.trig.AfterRows(ctx, tbl.tx.e, tbl.tx, nil, newRows)
			if err != nil {
				return -1, err
			}
		}

		if t.events&sql.UpdateEvent != 0 && tbl.updatedOldRows != nil {
			oldRows := &triggerRows{
				numCols: len(tbl.tt.Columns()),
				vals:    tbl.updatedOldRows,
			}
			newRows := &triggerRows{
				numCols: len(tbl.tt.Columns()),
				vals:    tbl.updatedNewRows,
			}
			err := t.trig.AfterRows(ctx, tbl.tx.e, tbl.tx, oldRows, newRows)
			if err != nil {
				return -1, err
			}
		}
	}

	return cnt, nil
}
