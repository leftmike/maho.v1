package engine

import (
	"context"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/leftmike/maho/sql"
)

const (
	fkTriggerType = "foreignKeyTriggerType"
)

type triggerDecoder func(buf []byte) (sql.Trigger, error)

var (
	TriggerDecoders = map[string]triggerDecoder{
		fkTriggerType: decodeFKTrigger,
	}
)

type trigger struct {
	typ    string
	events int64
	trig   sql.Trigger
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
	for _, trig := range tbl.tt.triggers {
		if trig.events&sql.DeleteEvent != 0 && tbl.deletedRows != nil {
			oldRows := &triggerRows{
				numCols: len(tbl.tt.Columns()),
				vals:    tbl.deletedRows,
			}
			err := trig.trig.AfterRows(ctx, tbl.tx, tbl, oldRows, nil)
			if err != nil {
				return -1, err
			}
		}

		if trig.events&sql.InsertEvent != 0 && tbl.insertedRows != nil {
			newRows := &triggerRows{
				numCols: len(tbl.tt.Columns()),
				vals:    tbl.insertedRows,
			}
			err := trig.trig.AfterRows(ctx, tbl.tx, tbl, nil, newRows)
			if err != nil {
				return -1, err
			}
		}

		if trig.events&sql.UpdateEvent != 0 && tbl.updatedOldRows != nil {
			oldRows := &triggerRows{
				numCols: len(tbl.tt.Columns()),
				vals:    tbl.updatedOldRows,
			}
			newRows := &triggerRows{
				numCols: len(tbl.tt.Columns()),
				vals:    tbl.updatedNewRows,
			}
			err := trig.trig.AfterRows(ctx, tbl.tx, tbl, oldRows, newRows)
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
	tn sql.TableName
	fk foreignKey
}

func (fkt *foreignKeyTrigger) Encode() ([]byte, error) {
	return proto.Marshal(&ForeignKeyTrigger{
		Table: &TableName{
			Database: fkt.tn.Database.String(),
			Schema:   fkt.tn.Schema.String(),
			Table:    fkt.tn.Table.String(),
		},
		ForeignKey: encodeForeignKey(fkt.fk),
	})
}

func decodeFKTrigger(buf []byte) (sql.Trigger, error) {
	var fkt ForeignKeyTrigger
	err := proto.Unmarshal(buf, &fkt)
	if err != nil {
		return nil, fmt.Errorf("engine: trigger type: %s: %s", fkTriggerType, err)
	}
	return &foreignKeyTrigger{
		tn: sql.TableName{
			Database: sql.QuotedID(fkt.Table.Database),
			Schema:   sql.QuotedID(fkt.Table.Schema),
			Table:    sql.QuotedID(fkt.Table.Table),
		},
		fk: decodeForeignKey(fkt.ForeignKey),
	}, nil
}

func (fkt *foreignKeyTrigger) AfterRows(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	rtt, err := tx.LookupTableType(ctx, fkt.fk.refTable)
	if err != nil {
		return err
	}
	rtbl, err := tx.LookupTable(ctx, fkt.fk.refTable, rtt.Version())
	if err != nil {
		return err
	}
	rpkey := rtt.PrimaryKey()

	refRow := make([]sql.Value, len(rtt.Columns()))
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
			err = r.Next(ctx, refRow)
			r.Close()
			if err == io.EOF {
				return fmt.Errorf("engine: table %s: insert violates foreign key constraint: %s",
					fkt.tn, fkt.fk.name)
			} else if err != nil {
				return err
			}
		} else {
			// XXX: lookup and use the index
		}
	}

	return nil
}
