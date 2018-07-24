package query

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/sql"
)

type ColumnUpdate struct {
	Column sql.Identifier
	Expr   expr.Expr
}

type Update struct {
	Table         sql.TableName
	ColumnUpdates []ColumnUpdate
	Where         expr.Expr
}

func (stmt *Update) String() string {
	s := fmt.Sprintf("UPDATE %s SET ", stmt.Table)
	for i, cu := range stmt.ColumnUpdates {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s = %s", cu.Column, cu.Expr)
	}
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

type columnUpdate struct {
	index int
	expr  expr.CExpr
}

type updatePlan struct {
	columns []sql.Identifier
	types   []db.ColumnType
	dest    []sql.Value
	rows    evaluate.Rows
	updates []columnUpdate
}

func (up *updatePlan) EvalRef(idx int) sql.Value {
	return up.dest[idx]
}

func (up *updatePlan) Execute(ses evaluate.Session, tx *engine.Transaction) (int64, error) {
	up.dest = make([]sql.Value, len(up.rows.Columns()))
	cnt := int64(0)
	updates := make([]db.ColumnUpdate, len(up.updates))

	for {
		err := up.rows.Next(ses, up.dest)
		if err == io.EOF {
			return cnt, nil
		} else if err != nil {
			return -1, err
		}
		for idx := range up.updates {
			cdx := up.updates[idx].index
			var val sql.Value
			val, err = up.updates[idx].expr.Eval(up)
			if err != nil {
				return -1, err
			}
			val, err = up.types[cdx].ConvertValue(up.columns[cdx], val)
			if err != nil {
				return -1, err
			}
			updates[idx] = db.ColumnUpdate{Index: cdx, Value: val}
		}
		err = up.rows.Update(ses, updates)
		if err != nil {
			return -1, err
		}
		cnt += 1
	}
}

func (stmt *Update) Plan(ses evaluate.Session, tx *engine.Transaction) (interface{}, error) {
	tbl, err := ses.Manager().LookupTable(ses, tx, stmt.Table.Database, stmt.Table.Table)
	if err != nil {
		return nil, err
	}
	er, err := tbl.Rows(ses)
	if err != nil {
		return nil, err
	}
	var rows evaluate.Rows
	rows = engineRows{er}
	fctx := makeFromContext(stmt.Table.Table, rows.Columns())
	if stmt.Where != nil {
		ce, err := expr.Compile(fctx, stmt.Where, false)
		if err != nil {
			return nil, err
		}
		rows = &filterRows{rows: rows, cond: ce}
	}

	plan := updatePlan{
		columns: tbl.Columns(ses),
		types:   tbl.ColumnTypes(ses),
		rows:    rows,
		updates: make([]columnUpdate, 0, len(stmt.ColumnUpdates)),
	}

	for _, cu := range stmt.ColumnUpdates {
		ce, err := expr.Compile(fctx, cu.Expr, false)
		if err != nil {
			return nil, err
		}
		cdx, err := fctx.colIndex(cu.Column, "update")
		if err != nil {
			return nil, err
		}
		plan.updates = append(plan.updates, columnUpdate{index: cdx, expr: ce})
	}
	return &plan, nil
}
