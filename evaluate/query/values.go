package query

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

type Values struct {
	Expressions [][]expr.Expr
}

func (stmt *Values) String() string {
	s := "VALUES"
	for i, r := range stmt.Expressions {
		if i > 0 {
			s += ", ("
		} else {
			s += " ("
		}

		for j, v := range r {
			if j > 0 {
				s += ", "
			}
			s += v.String()
		}

		s += ")"
	}

	return s
}

func (stmt *Values) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction) (evaluate.Plan, error) {

	cols := make([]sql.Identifier, len(stmt.Expressions[0]))
	for i := 0; i < len(cols); i++ {
		cols[i] = sql.ID(fmt.Sprintf("column%d", i+1))
	}

	var rows [][]sql.CExpr
	for _, r := range stmt.Expressions {
		row := make([]sql.CExpr, len(r))
		for j := range r {
			var err error
			row[j], err = expr.Compile(ctx, pctx, tx, nil, r[j])
			if err != nil {
				return nil, err
			}
		}
		rows = append(rows, row)
	}
	return valuesPlan{
		cols: cols,
		rows: rows,
	}, nil
}

type valuesPlan struct {
	cols []sql.Identifier
	rows [][]sql.CExpr
}

func (_ valuesPlan) Planned() {}

func (vp valuesPlan) Columns() []sql.Identifier {
	return vp.cols
}

func (vp valuesPlan) Rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	return &exprValues{
		tx:      tx,
		numCols: len(vp.cols),
		rows:    vp.rows,
	}, nil
}

type exprValues struct {
	tx      sql.Transaction
	numCols int
	rows    [][]sql.CExpr
	index   int
}

func (ev *exprValues) NumColumns() int {
	return ev.numCols
}

func (ev *exprValues) Close() error {
	ev.index = len(ev.rows)
	return nil
}

func (ev *exprValues) Next(ctx context.Context, dest []sql.Value) error {
	if ev.index == len(ev.rows) {
		return io.EOF
	}

	row := ev.rows[ev.index]
	ev.index += 1

	i := 0
	for i < len(dest) && i < len(row) {
		var err error
		dest[i], err = row[i].Eval(ctx, ev.tx, nil)
		if err != nil {
			return err
		}
		i += 1
	}

	return nil
}

func (_ *exprValues) Delete(ctx context.Context) error {
	return errors.New("values: rows may not be deleted")
}

func (_ *exprValues) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	return errors.New("values: rows may not be updated")
}
