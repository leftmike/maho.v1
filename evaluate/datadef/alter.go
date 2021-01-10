package datadef

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type AddConstraint struct {
	Table      sql.TableName
	ForeignKey *ForeignKey
}

func (stmt *AddConstraint) String() string {
	return fmt.Sprintf("ALTER TABLE %s ADD %s", stmt.Table, stmt.ForeignKey)
}

func (stmt *AddConstraint) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction, cctx sql.CompileContext) (evaluate.Plan, error) {

	stmt.Table = pctx.ResolveTableName(stmt.Table)
	stmt.ForeignKey.FKTable = stmt.Table
	err := stmt.ForeignKey.plan(ctx, pctx, tx)
	if err != nil {
		return nil, err
	}

	return stmt, nil
}

func (_ *AddConstraint) Tag() string {
	return "ALTER TABLE"
}

func (stmt *AddConstraint) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	return -1, stmt.ForeignKey.execute(ctx, tx, true)
}
