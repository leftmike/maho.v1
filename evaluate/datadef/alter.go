package datadef

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type AddConstraint struct {
	Table      sql.TableName
	IfExists   bool
	ForeignKey *ForeignKey
}

func (stmt *AddConstraint) String() string {
	s := "ALTER TABLE "
	if stmt.IfExists {
		s += "IF EXISTS "
	}
	s += fmt.Sprintf("%s ADD %s", stmt.Table, stmt.ForeignKey)
	return s
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
	return -1, stmt.ForeignKey.execute(ctx, tx)
}
