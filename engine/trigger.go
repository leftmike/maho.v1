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
	tn sql.TableName
	fk foreignKey

	sqlStmt string
	prep    *evaluate.PreparedRowsPlan
}

func (fkt *foreignKeyTrigger) Type() string {
	return fkTriggerType
}

func (fkt *foreignKeyTrigger) Encode() ([]byte, error) {
	return proto.Marshal(&ForeignKeyTrigger{
		Table: &TableName{
			Database: fkt.tn.Database.String(),
			Schema:   fkt.tn.Schema.String(),
			Table:    fkt.tn.Table.String(),
		},
		ForeignKey: encodeForeignKey(fkt.fk),
		SqlStmt:    fkt.sqlStmt,
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
		fk:      decodeForeignKey(fkt.ForeignKey),
		sqlStmt: fkt.SqlStmt,
	}, nil
}

func (fkt *foreignKeyTrigger) AfterRows(ctx context.Context, tx sql.Transaction, tbl sql.Table,
	oldRows, newRows sql.Rows) error {

	if fkt.prep == nil {
		p := parser.NewParser(strings.NewReader(fkt.sqlStmt), fkt.sqlStmt)
		stmt, err := p.Parse()
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key: %s: %s", fkt.tn, fkt.fk.name, err))
		}
		prep, err := evaluate.PreparePlan(ctx, stmt, planContext{tx.(*transaction).e}, tx)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key: %s: %s", fkt.tn, fkt.fk.name, err))
		}
		fkt.prep = prep.(*evaluate.PreparedRowsPlan)
	}

	params := make([]sql.Value, len(fkt.fk.keyCols))
	row := make([]sql.Value, newRows.NumColumns())
	for {
		err := newRows.Next(ctx, row)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		var hasNull bool
		for kdx, col := range fkt.fk.keyCols {
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
			panic(fmt.Sprintf("engine: table %s: foreign key: %s: %s", fkt.tn, fkt.fk.name, err))
		}

		rows, err := fkt.prep.Rows(ctx, tx)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key: %s: %s", fkt.tn, fkt.fk.name, err))
		}

		cntRow := []sql.Value{nil}
		err = rows.Next(ctx, cntRow)
		if err != nil {
			panic(fmt.Sprintf("engine: table %s: foreign key: %s: %s", fkt.tn, fkt.fk.name, err))
		}
		rows.Close()
		cnt := cntRow[0].(sql.Int64Value)
		if cnt == 0 {
			return fmt.Errorf(
				"engine: table %s: insert or update violates foreign key constraint: %s",
				fkt.tn, fkt.fk.name)
		}
	}

	return nil
}
