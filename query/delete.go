package query

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/sql"
)

type Delete struct {
	Table sql.TableName
	Where expr.Expr
}

func (stmt *Delete) String() string {
	s := fmt.Sprintf("DELETE FROM %s", stmt.Table)
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

type deletePlan struct {
	rows db.Rows
}

func (dp *deletePlan) Execute(ctx context.Context, tx engine.Transaction) (int64, error) {
	dest := make([]sql.Value, len(dp.rows.Columns()))
	cnt := int64(0)
	for {
		err := dp.rows.Next(ctx, dest)
		if err == io.EOF {
			return cnt, nil
		} else if err != nil {
			return cnt, err
		}
		err = dp.rows.Delete(ctx)
		if err != nil {
			return cnt, err
		}
		cnt += 1
	}
}

func (stmt *Delete) Plan(ctx context.Context, tx engine.Transaction) (interface{}, error) {
	tbl, err := engine.LookupTable(ctx, tx, stmt.Table.Database, stmt.Table.Table)
	if err != nil {
		return nil, err
	}

	rows, err := tbl.Rows()
	if err != nil {
		return nil, err
	}
	if stmt.Where != nil {
		ce, err := expr.Compile(makeFromContext(stmt.Table.Table, rows.Columns()), stmt.Where,
			false)
		if err != nil {
			return nil, err
		}
		rows = &filterRows{rows: rows, cond: ce}
	}
	return &deletePlan{rows: rows}, nil
}