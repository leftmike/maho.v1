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
	fkMatchTrigger    = "fkMatchTrigger"
	fkRestrictTrigger = "fkRestrictTrigger"
	fkDeleteTrigger   = "fkDeleteTrigger"
	fkUpdateTrigger   = "fkUpdateTrigger"
	fkSetTrigger      = "fkSetTrigger"
)

type triggerDecoder func(buf []byte) (sql.Trigger, error)

var (
	TriggerDecoders = map[string]triggerDecoder{
		fkMatchTrigger:    decodeFKTrigger,
		fkRestrictTrigger: decodeFKTrigger,
		fkDeleteTrigger:   decodeFKTrigger,
		fkUpdateTrigger:   decodeFKTrigger,
		fkSetTrigger:      decodeFKTrigger,
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
	typ     string
	con     sql.Identifier
	fktn    sql.TableName
	rtn     sql.TableName
	keyCols []int
	sqlStmt string
	prep    evaluate.PreparedPlan
}

func (fkt *fkTrigger) Type() string {
	return fkt.typ
}

func (fkt *fkTrigger) Encode() ([]byte, error) {
	return proto.Marshal(&FKTrigger{
		Type:       fkt.typ,
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

func decodeFKTrigger(buf []byte) (sql.Trigger, error) {
	var fkt FKTrigger
	err := proto.Unmarshal(buf, &fkt)
	if err != nil {
		return nil, fmt.Errorf("engine: foreign key trigger: %s", err)
	}

	return &fkTrigger{
		typ: fkt.Type,
		con: sql.QuotedID(fkt.Constraint),
		fktn: sql.TableName{
			Database: sql.QuotedID(fkt.FKeyTable.Database),
			Schema:   sql.QuotedID(fkt.FKeyTable.Schema),
			Table:    sql.QuotedID(fkt.FKeyTable.Table),
		},
		rtn: sql.TableName{
			Database: sql.QuotedID(fkt.RefTable.Database),
			Schema:   sql.QuotedID(fkt.RefTable.Schema),
			Table:    sql.QuotedID(fkt.RefTable.Table),
		},
		keyCols: decodeIntSlice(fkt.KeyColumns),
		sqlStmt: fkt.SQLStmt,
	}, nil
}

func (fkt *fkTrigger) AfterRows(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	if fkt.prep == nil {
		p := parser.NewParser(strings.NewReader(fkt.sqlStmt), fkt.sqlStmt)
		stmt, err := p.Parse()
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key: %s: %s", fkt.fktn, fkt.con, err))
		}
		fkt.prep, err = evaluate.PreparePlan(ctx, stmt, planContext{tx.(*transaction).e}, tx)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key: %s: %s", fkt.fktn, fkt.con, err))
		}
	}

	switch fkt.typ {
	case fkMatchTrigger:
		return fkt.afterRowsMatch(ctx, tx, tbl, oldRows, newRows)
	case fkRestrictTrigger:
		return fkt.afterRowsRestrict(ctx, tx, tbl, oldRows, newRows)
	case fkDeleteTrigger:
		return fkt.afterRowsDelete(ctx, tx, tbl, oldRows, newRows)
	case fkUpdateTrigger:
		return fkt.afterRowsUpdate(ctx, tx, tbl, oldRows, newRows)
	//case fkSetTrigger:
	//	return fkt.afterRowsSet(ctx, tx, tbl, oldRows, newRows)
	default:
		panic(fmt.Sprintf("engine: table %s: foreign key: %s: unknown trigger type: %s",
			fkt.fktn, fkt.con, fkt.typ))
	}
}

func (fkt *fkTrigger) afterRowsMatch(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	prep := fkt.prep.(*evaluate.PreparedRowsPlan)
	params := make([]sql.Value, len(fkt.keyCols))
	row := make([]sql.Value, newRows.NumColumns())
	for {
		err := newRows.Next(ctx, row)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		var hasNull bool
		for cdx, col := range fkt.keyCols {
			if row[col] == nil {
				hasNull = true
				break
			}
			params[cdx] = row[col]
		}
		if hasNull {
			continue
		}

		err = prep.SetParameters(params)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key match: %s: %s", fkt.fktn, fkt.con,
				err))
		}

		rows, err := prep.Rows(ctx, tx)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key match: %s: %s", fkt.fktn, fkt.con,
				err))
		}

		cntRow := []sql.Value{nil}
		err = rows.Next(ctx, cntRow)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key match: %s: %s", fkt.fktn, fkt.con,
				err))
		}
		rows.Close()
		cnt := cntRow[0].(sql.Int64Value)
		if cnt == 0 {
			return fmt.Errorf(
				"engine: table %s: insert or update violates foreign key constraint: %s",
				fkt.fktn, fkt.con)
		}
	}

	return nil
}

func (fkt *fkTrigger) afterRowsRestrict(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	prep := fkt.prep.(*evaluate.PreparedRowsPlan)
	params := make([]sql.Value, len(fkt.keyCols))
	row := make([]sql.Value, oldRows.NumColumns())
	for {
		err := oldRows.Next(ctx, row)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		for cdx, col := range fkt.keyCols {
			params[cdx] = row[col]
		}

		err = prep.SetParameters(params)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key restrict: %s: %s", fkt.fktn, fkt.con,
				err))
		}

		rows, err := prep.Rows(ctx, tx)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key restrict: %s: %s", fkt.fktn, fkt.con,
				err))
		}

		cntRow := []sql.Value{nil}
		err = rows.Next(ctx, cntRow)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key restrict: %s: %s", fkt.fktn, fkt.con,
				err))
		}
		rows.Close()
		cnt := cntRow[0].(sql.Int64Value)
		if cnt > 0 {
			return fmt.Errorf(
				"engine: table %s: delete or update violates foreign key constraint %s on table %s",
				fkt.fktn, fkt.con, fkt.rtn)
		}
	}

	return nil
}

func (fkt *fkTrigger) afterRowsDelete(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	prep := fkt.prep.(*evaluate.PreparedStmtPlan)
	params := make([]sql.Value, len(fkt.keyCols))
	row := make([]sql.Value, oldRows.NumColumns())
	for {
		err := oldRows.Next(ctx, row)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		for cdx, col := range fkt.keyCols {
			params[cdx] = row[col]
		}

		err = prep.SetParameters(params)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key restrict: %s: %s", fkt.fktn, fkt.con,
				err))
		}

		_, err = prep.Execute(ctx, tx)
		if err != nil {
			return fmt.Errorf(
				"engine: table %s: delete violates foreign key constraint %s on table %s",
				fkt.fktn, fkt.con, fkt.rtn)
		}
	}

	return nil
}

func (fkt *fkTrigger) afterRowsUpdate(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	prep := fkt.prep.(*evaluate.PreparedStmtPlan)
	params := make([]sql.Value, len(fkt.keyCols)*2)
	row := make([]sql.Value, oldRows.NumColumns())
	for {
		err := oldRows.Next(ctx, row)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		for cdx, col := range fkt.keyCols {
			params[cdx] = row[col]
		}

		err = newRows.Next(ctx, row)
		if err != nil {
			return err
		}

		for cdx, col := range fkt.keyCols {
			params[cdx+len(fkt.keyCols)] = row[col]
		}

		err = prep.SetParameters(params)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key restrict: %s: %s", fkt.fktn, fkt.con,
				err))
		}

		_, err = prep.Execute(ctx, tx)
		if err != nil {
			return fmt.Errorf(
				"engine: table %s: update violates foreign key constraint %s on table %s",
				fkt.fktn, fkt.con, fkt.rtn)
		}
	}

	return nil
}
