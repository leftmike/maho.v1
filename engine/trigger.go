package engine

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/flags"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

const (
	fkMatchTriggerType    = "fkMatchTriggerType"
	fkRestrictTriggerType = "fkRestrictTriggerType"
)

type triggerDecoder func(buf []byte) (sql.Trigger, error)

var (
	TriggerDecoders = map[string]triggerDecoder{
		fkMatchTriggerType:    decodeFKMatchTrigger,
		fkRestrictTriggerType: decodeFKRestrictTrigger,
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

func (tx *transaction) stmtTriggers(ctx context.Context) error {
	for len(tx.modified) > 0 {
		tbl := tx.modified[0]
		tx.modified = tx.modified[1:]

		deletedRows := tbl.deletedRows
		insertedRows := tbl.insertedRows
		updatedOldRows := tbl.updatedOldRows
		updatedNewRows := tbl.updatedNewRows

		tbl.deletedRows = nil
		tbl.insertedRows = nil
		tbl.updatedOldRows = nil
		tbl.updatedNewRows = nil
		tbl.modified = false

		numCols := len(tbl.tt.Columns())
		for _, trig := range tbl.tt.triggers {
			if trig.events&sql.DeleteEvent != 0 && deletedRows != nil {
				oldRows := &triggerRows{
					numCols: numCols,
					vals:    deletedRows,
				}
				err := trig.trig.AfterRows(ctx, tbl.tx, tbl, oldRows, nil)
				if err != nil {
					return err
				}
			}

			if trig.events&sql.InsertEvent != 0 && insertedRows != nil {
				newRows := &triggerRows{
					numCols: numCols,
					vals:    insertedRows,
				}
				err := trig.trig.AfterRows(ctx, tbl.tx, tbl, nil, newRows)
				if err != nil {
					return err
				}
			}

			if trig.events&sql.UpdateEvent != 0 && updatedOldRows != nil {
				oldRows := &triggerRows{
					numCols: numCols,
					vals:    updatedOldRows,
				}
				newRows := &triggerRows{
					numCols: numCols,
					vals:    updatedNewRows,
				}
				err := trig.trig.AfterRows(ctx, tbl.tx, tbl, oldRows, newRows)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type planContext struct {
	e *Engine
}

func (pctx planContext) GetFlag(f flags.Flag) bool {
	return pctx.e.GetFlag(f)
}
func (_ planContext) ResolveTableName(tn sql.TableName) sql.TableName {
	return tn
}

func (_ planContext) ResolveSchemaName(sn sql.SchemaName) sql.SchemaName {
	panic("unexpected, should never be called")
}

func (_ planContext) PlanParameter(num int) (*sql.Value, error) {
	panic("unexpected, should never be called")
}

func (_ planContext) SetPreparedPlan(nam sql.Identifier, prep evaluate.PreparedPlan) error {
	panic("unexpected, should never be called")
}

func (_ planContext) GetPreparedPlan(nam sql.Identifier) evaluate.PreparedPlan {
	panic("unexpected, should never be called")
}

type fkTrigger struct {
	con     sql.Identifier
	fktn    sql.TableName
	rtn     sql.TableName
	keyCols []int
	sqlStmt string
}

func (fkt *fkTrigger) Encode() ([]byte, error) {
	return proto.Marshal(&FKTrigger{
		Constraint: fkt.con.String(),
		FKeyTable: &TableName{
			Database: fkt.fktn.Database.String(),
			Schema:   fkt.fktn.Schema.String(),
			Table:    fkt.fktn.Table.String(),
		},
		RefTable: &TableName{
			Database: fkt.rtn.Database.String(),
			Schema:   fkt.rtn.Schema.String(),
			Table:    fkt.rtn.Table.String(),
		},
		KeyColumns: encodeIntSlice(fkt.keyCols),
		SQLStmt:    fkt.sqlStmt,
	})
}

func decodeFKTrigger(buf []byte, fkt *fkTrigger) error {
	var fktmd FKTrigger
	err := proto.Unmarshal(buf, &fktmd)
	if err != nil {
		return fmt.Errorf("engine: foreign key trigger: %s", err)
	}

	fkt.con = sql.QuotedID(fktmd.Constraint)
	fkt.fktn = sql.TableName{
		Database: sql.QuotedID(fktmd.FKeyTable.Database),
		Schema:   sql.QuotedID(fktmd.FKeyTable.Schema),
		Table:    sql.QuotedID(fktmd.FKeyTable.Table),
	}
	fkt.rtn = sql.TableName{
		Database: sql.QuotedID(fktmd.RefTable.Database),
		Schema:   sql.QuotedID(fktmd.RefTable.Schema),
		Table:    sql.QuotedID(fktmd.RefTable.Table),
	}
	fkt.keyCols = decodeIntSlice(fktmd.KeyColumns)
	fkt.sqlStmt = fktmd.SQLStmt
	return nil
}

type fkMatchTrigger struct {
	fkTrigger
	prep *evaluate.PreparedRowsPlan
}

func (fkm *fkMatchTrigger) Type() string {
	return fkMatchTriggerType
}

func decodeFKMatchTrigger(buf []byte) (sql.Trigger, error) {
	var fkm fkMatchTrigger
	err := decodeFKTrigger(buf, &fkm.fkTrigger)
	if err != nil {
		return nil, err
	}
	return &fkm, nil
}

func (fkm *fkMatchTrigger) AfterRows(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	if fkm.prep == nil {
		p := parser.NewParser(strings.NewReader(fkm.sqlStmt), fkm.sqlStmt)
		stmt, err := p.Parse()
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key match: %s: %s", fkm.fktn, fkm.con,
				err))
		}
		prep, err := evaluate.PreparePlan(ctx, stmt, planContext{tx.(*transaction).e}, tx)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key match: %s: %s", fkm.fktn, fkm.con,
				err))
		}
		fkm.prep = prep.(*evaluate.PreparedRowsPlan)
	}

	params := make([]sql.Value, len(fkm.keyCols))
	row := make([]sql.Value, newRows.NumColumns())
	for {
		err := newRows.Next(ctx, row)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		var hasNull bool
		for cdx, col := range fkm.keyCols {
			if row[col] == nil {
				hasNull = true
				break
			}
			params[cdx] = row[col]
		}
		if hasNull {
			continue
		}

		err = fkm.prep.SetParameters(params)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key match: %s: %s", fkm.fktn, fkm.con,
				err))
		}

		rows, err := fkm.prep.Rows(ctx, tx)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key match: %s: %s", fkm.fktn, fkm.con,
				err))
		}

		cntRow := []sql.Value{nil}
		err = rows.Next(ctx, cntRow)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key match: %s: %s", fkm.fktn, fkm.con,
				err))
		}
		rows.Close()
		cnt := cntRow[0].(sql.Int64Value)
		if cnt == 0 {
			return fmt.Errorf(
				"engine: table %s: insert or update violates foreign key constraint: %s",
				fkm.fktn, fkm.con)
		}
	}

	return nil
}

type fkRestrictTrigger struct {
	fkTrigger
	prep *evaluate.PreparedRowsPlan
}

func (fkr *fkRestrictTrigger) Type() string {
	return fkRestrictTriggerType
}

func decodeFKRestrictTrigger(buf []byte) (sql.Trigger, error) {
	var fkr fkRestrictTrigger
	err := decodeFKTrigger(buf, &fkr.fkTrigger)
	if err != nil {
		return nil, err
	}
	return &fkr, nil
}

func (fkr *fkRestrictTrigger) AfterRows(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	if fkr.prep == nil {
		p := parser.NewParser(strings.NewReader(fkr.sqlStmt), fkr.sqlStmt)
		stmt, err := p.Parse()
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key restrict: %s: %s", fkr.fktn, fkr.con,
				err))
		}
		prep, err := evaluate.PreparePlan(ctx, stmt, planContext{tx.(*transaction).e}, tx)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key restrict: %s: %s", fkr.fktn, fkr.con,
				err))
		}
		fkr.prep = prep.(*evaluate.PreparedRowsPlan)
	}

	params := make([]sql.Value, len(fkr.keyCols))
	row := make([]sql.Value, oldRows.NumColumns())
	for {
		err := oldRows.Next(ctx, row)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		for cdx, col := range fkr.keyCols {
			params[cdx] = row[col]
		}

		err = fkr.prep.SetParameters(params)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key restrict: %s: %s", fkr.fktn, fkr.con,
				err))
		}

		rows, err := fkr.prep.Rows(ctx, tx)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key restrict: %s: %s", fkr.fktn, fkr.con,
				err))
		}

		cntRow := []sql.Value{nil}
		err = rows.Next(ctx, cntRow)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key restrict: %s: %s", fkr.fktn, fkr.con,
				err))
		}
		rows.Close()
		cnt := cntRow[0].(sql.Int64Value)
		if cnt > 0 {
			return fmt.Errorf(
				"engine: table %s: delete or update violates foreign key constraint %s on table %s",
				fkr.fktn, fkr.con, fkr.rtn)
		}
	}

	return nil
}
