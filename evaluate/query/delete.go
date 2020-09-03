package query

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
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
	tn    sql.TableName
	ttVer int64
	where sql.CExpr
}

func (stmt *Delete) Resolve(ses *evaluate.Session) {
	stmt.Table = ses.ResolveTableName(stmt.Table)
}

func (stmt *Delete) Plan(ctx context.Context, tx sql.Transaction) (evaluate.Plan, error) {
	tt, err := tx.LookupTableType(ctx, stmt.Table)
	if err != nil {
		return nil, err
	}

	var where sql.CExpr
	if stmt.Where != nil {
		where, err = expr.Compile(ctx, tx, makeFromContext(stmt.Table.Table, tt.Columns()),
			stmt.Where)
		if err != nil {
			return nil, err
		}
	}
	return &deletePlan{stmt.Table, tt.Version(), where}, nil
}

func (dp *deletePlan) Explain() string {
	return fmt.Sprintf("delete from %s where %s", dp.tn, dp.where)
}

func (dp *deletePlan) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	tbl, err := tx.LookupTable(ctx, dp.tn, dp.ttVer)
	if err != nil {
		return -1, err
	}
	rows, err := tbl.Rows(ctx, nil, nil)
	if err != nil {
		return -1, err
	}
	if dp.where != nil {
		rows = &filterRows{tx: tx, rows: rows, cond: dp.where}
	}
	defer rows.Close()

	dest := make([]sql.Value, len(rows.Columns()))
	var cnt int64
	for {
		err := rows.Next(ctx, dest)
		if err == io.EOF {
			return cnt, nil
		} else if err != nil {
			return cnt, err
		}
		err = rows.Delete(ctx)
		if err != nil {
			return cnt, err
		}
		cnt += 1
	}
}
