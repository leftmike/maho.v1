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

type fkMatchTrigger struct {
	con     sql.Identifier
	fktn    sql.TableName
	keyCols []int
	sqlStmt string
	prep    *evaluate.PreparedRowsPlan
}

func (fkm *fkMatchTrigger) Type() string {
	return fkMatchTriggerType
}

func (fkm *fkMatchTrigger) Encode() ([]byte, error) {
	return proto.Marshal(&FKMatchTrigger{
		Constraint: fkm.con.String(),
		FKeyTable: &TableName{
			Database: fkm.fktn.Database.String(),
			Schema:   fkm.fktn.Schema.String(),
			Table:    fkm.fktn.Table.String(),
		},
		KeyColumns: encodeIntSlice(fkm.keyCols),
		SQLStmt:    fkm.sqlStmt,
	})
}

func decodeFKMatchTrigger(buf []byte) (sql.Trigger, error) {
	var fkm FKMatchTrigger
	err := proto.Unmarshal(buf, &fkm)
	if err != nil {
		return nil, fmt.Errorf("engine: trigger type: %s: %s", fkMatchTriggerType, err)
	}
	return &fkMatchTrigger{
		con: sql.QuotedID(fkm.Constraint),
		fktn: sql.TableName{
			Database: sql.QuotedID(fkm.FKeyTable.Database),
			Schema:   sql.QuotedID(fkm.FKeyTable.Schema),
			Table:    sql.QuotedID(fkm.FKeyTable.Table),
		},
		keyCols: decodeIntSlice(fkm.KeyColumns),
		sqlStmt: fkm.SQLStmt,
	}, nil
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
	con     sql.Identifier
	fktn    sql.TableName
	rtn     sql.TableName
	keyCols []int
	sqlStmt string
	prep    *evaluate.PreparedRowsPlan
}

func (fkr *fkRestrictTrigger) Type() string {
	return fkRestrictTriggerType
}

func (fkr *fkRestrictTrigger) Encode() ([]byte, error) {
	return proto.Marshal(&FKRestrictTrigger{
		Constraint: fkr.con.String(),
		FKeyTable: &TableName{
			Database: fkr.fktn.Database.String(),
			Schema:   fkr.fktn.Schema.String(),
			Table:    fkr.fktn.Table.String(),
		},
		RefTable: &TableName{
			Database: fkr.rtn.Database.String(),
			Schema:   fkr.rtn.Schema.String(),
			Table:    fkr.rtn.Table.String(),
		},
		KeyColumns: encodeIntSlice(fkr.keyCols),
		SQLStmt:    fkr.sqlStmt,
	})
}

func decodeFKRestrictTrigger(buf []byte) (sql.Trigger, error) {
	var fkr FKRestrictTrigger
	err := proto.Unmarshal(buf, &fkr)
	if err != nil {
		return nil, fmt.Errorf("engine: trigger type: %s: %s", fkRestrictTriggerType, err)
	}
	return &fkRestrictTrigger{
		con: sql.QuotedID(fkr.Constraint),
		fktn: sql.TableName{
			Database: sql.QuotedID(fkr.FKeyTable.Database),
			Schema:   sql.QuotedID(fkr.FKeyTable.Schema),
			Table:    sql.QuotedID(fkr.FKeyTable.Table),
		},
		rtn: sql.TableName{
			Database: sql.QuotedID(fkr.RefTable.Database),
			Schema:   sql.QuotedID(fkr.RefTable.Schema),
			Table:    sql.QuotedID(fkr.RefTable.Table),
		},
		keyCols: decodeIntSlice(fkr.KeyColumns),
		sqlStmt: fkr.SQLStmt,
	}, nil
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
