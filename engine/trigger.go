package engine

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/sql"
)

type trigger interface {
	afterRows(ctx context.Context, tbl *table, oldRows, newRows sql.Rows) error
}

type triggerConfig struct {
	events int64
	trig   trigger
}

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

func (tr *triggerRows) Next(ctx context.Context, dest []sql.Value) error {
	if len(tr.vals) == 0 {
		return io.EOF
	}
	copy(dest, tr.vals[0])
	tr.vals = tr.vals[1:]
	return nil
}

func (tr *triggerRows) Delete(ctx context.Context) error {
	panic("engine: trigger rows may not be deleted")
}

func (tr *triggerRows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
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
	for _, tc := range tbl.tt.triggers {
		if tc.events&sql.DeleteEvent != 0 && tbl.deletedRows != nil {
			oldRows := &triggerRows{
				numCols: len(tbl.tt.Columns()),
				vals:    tbl.deletedRows,
			}
			err := tc.trig.afterRows(ctx, tbl, oldRows, nil)
			if err != nil {
				return -1, err
			}
		}

		if tc.events&sql.InsertEvent != 0 && tbl.insertedRows != nil {
			newRows := &triggerRows{
				numCols: len(tbl.tt.Columns()),
				vals:    tbl.insertedRows,
			}
			err := tc.trig.afterRows(ctx, tbl, nil, newRows)
			if err != nil {
				return -1, err
			}
		}

		if tc.events&sql.UpdateEvent != 0 && tbl.updatedOldRows != nil {
			oldRows := &triggerRows{
				numCols: len(tbl.tt.Columns()),
				vals:    tbl.updatedOldRows,
			}
			newRows := &triggerRows{
				numCols: len(tbl.tt.Columns()),
				vals:    tbl.updatedNewRows,
			}
			err := tc.trig.afterRows(ctx, tbl, oldRows, newRows)
			if err != nil {
				return -1, err
			}
		}
	}

	return cnt, nil
}

func hasNullColumns(fk foreignKey, row []sql.Value) bool {
	for _, col := range fk.keyCols {
		if row[col] == nil {
			return true
		}
	}

	return false
}

type foreignKeyTrigger struct {
	fk foreignKey
}

func (fkt *foreignKeyTrigger) afterRows(ctx context.Context, tbl *table,
	oldRows, newRows sql.Rows) error {

	rtbl, rtt, err := tbl.tx.e.st.LookupTable(ctx, tbl.tx.tx, fkt.fk.refTable)
	if err != nil {
		return err
	}
	rpkey := rtt.PrimaryKey()

	row := make([]sql.Value, newRows.NumColumns())
	for {
		err := newRows.Next(ctx, row)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if hasNullColumns(fkt.fk, row) {
			continue
		}

		if fkt.fk.refIndex == 0 {
			keyRow := make([]sql.Value, len(rtt.Columns()))
			for cdx, col := range fkt.fk.keyCols {
				keyRow[rpkey[cdx].Column()] = row[col]
			}

			r, err := rtbl.Rows(ctx, keyRow, keyRow)
			if err != nil {
				return err
			}
			_, err = r.Next(ctx)
			r.Close()
			if err == io.EOF {
				return fmt.Errorf("engine: table %s: insert violates foreign key constraint: %s",
					tbl.tn, fkt.fk.name)
			} else if err != nil {
				return err
			}
		} else {
			// XXX: lookup and use the index
		}
	}

	return nil
}
