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
	foreignKeyTriggerType = "foreignKeyTriggerType"
	foreignRefTriggerType = "foreignRefTriggerType"
)

type triggerDecoder func(buf []byte) (sql.Trigger, error)

var (
	TriggerDecoders = map[string]triggerDecoder{
		foreignKeyTriggerType: decodeFKeyTrigger,
		foreignRefTriggerType: decodeFRefTrigger,
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

type foreignKeyTrigger struct {
	name    sql.Identifier
	tn      sql.TableName
	keyCols []int
	sqlStmt string
	prep    *evaluate.PreparedRowsPlan
}

func (fkt *foreignKeyTrigger) Type() string {
	return foreignKeyTriggerType
}

func (fkt *foreignKeyTrigger) Encode() ([]byte, error) {
	return proto.Marshal(&ForeignKeyTrigger{
		Name: fkt.name.String(),
		Table: &TableName{
			Database: fkt.tn.Database.String(),
			Schema:   fkt.tn.Schema.String(),
			Table:    fkt.tn.Table.String(),
		},
		KeyColumns: encodeIntSlice(fkt.keyCols),
		SQLStmt:    fkt.sqlStmt,
	})
}

func decodeFKeyTrigger(buf []byte) (sql.Trigger, error) {
	var fkt ForeignKeyTrigger
	err := proto.Unmarshal(buf, &fkt)
	if err != nil {
		return nil, fmt.Errorf("engine: trigger type: %s: %s", foreignKeyTriggerType, err)
	}
	return &foreignKeyTrigger{
		name: sql.QuotedID(fkt.Name),
		tn: sql.TableName{
			Database: sql.QuotedID(fkt.Table.Database),
			Schema:   sql.QuotedID(fkt.Table.Schema),
			Table:    sql.QuotedID(fkt.Table.Table),
		},
		keyCols: decodeIntSlice(fkt.KeyColumns),
		sqlStmt: fkt.SQLStmt,
	}, nil
}

func (fkt *foreignKeyTrigger) AfterRows(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	if fkt.prep == nil {
		p := parser.NewParser(strings.NewReader(fkt.sqlStmt), fkt.sqlStmt)
		stmt, err := p.Parse()
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key: %s: %s", fkt.tn, fkt.name, err))
		}
		prep, err := evaluate.PreparePlan(ctx, stmt, planContext{tx.(*transaction).e}, tx)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key: %s: %s", fkt.tn, fkt.name, err))
		}
		fkt.prep = prep.(*evaluate.PreparedRowsPlan)
	}

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
		for kdx, col := range fkt.keyCols {
			if row[col] == nil {
				hasNull = true
				break
			}
			params[kdx] = row[col]
		}
		if hasNull {
			continue
		}

		err = fkt.prep.SetParameters(params)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key: %s: %s", fkt.tn, fkt.name, err))
		}

		rows, err := fkt.prep.Rows(ctx, tx)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key: %s: %s", fkt.tn, fkt.name, err))
		}

		cntRow := []sql.Value{nil}
		err = rows.Next(ctx, cntRow)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key: %s: %s", fkt.tn, fkt.name, err))
		}
		rows.Close()
		cnt := cntRow[0].(sql.Int64Value)
		if cnt == 0 {
			return fmt.Errorf(
				"engine: table %s: insert or update violates foreign key constraint: %s",
				fkt.tn, fkt.name)
		}
	}

	return nil
}

type foreignRefTrigger struct {
	name     sql.Identifier
	tn       sql.TableName
	keyCols  []int
	onDelete sql.RefAction
	onUpdate sql.RefAction
	delSQL   string
	updSQL   string
	delPrep  evaluate.PreparedPlan
	updPrep  evaluate.PreparedPlan
}

func (frt *foreignRefTrigger) Type() string {
	return foreignRefTriggerType
}

func (frt *foreignRefTrigger) Encode() ([]byte, error) {
	return proto.Marshal(&ForeignRefTrigger{
		Name: frt.name.String(),
		Table: &TableName{
			Database: frt.tn.Database.String(),
			Schema:   frt.tn.Schema.String(),
			Table:    frt.tn.Table.String(),
		},
		KeyColumns: encodeIntSlice(frt.keyCols),
		OnDelete:   int32(frt.onDelete),
		OnUpdate:   int32(frt.onUpdate),
		DeleteSQL:  frt.delSQL,
		UpdateSQL:  frt.updSQL,
	})
}

func decodeFRefTrigger(buf []byte) (sql.Trigger, error) {
	var frt ForeignRefTrigger
	err := proto.Unmarshal(buf, &frt)
	if err != nil {
		return nil, fmt.Errorf("engine: trigger type: %s: %s", foreignRefTriggerType, err)
	}
	return &foreignRefTrigger{
		name: sql.QuotedID(frt.Name),
		tn: sql.TableName{
			Database: sql.QuotedID(frt.Table.Database),
			Schema:   sql.QuotedID(frt.Table.Schema),
			Table:    sql.QuotedID(frt.Table.Table),
		},
		keyCols:  decodeIntSlice(frt.KeyColumns),
		onDelete: sql.RefAction(frt.OnDelete),
		onUpdate: sql.RefAction(frt.OnUpdate),
		delSQL:   frt.DeleteSQL,
		updSQL:   frt.UpdateSQL,
	}, nil
}

func (frt *foreignRefTrigger) AfterRows(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	// XXX
	return nil
}
