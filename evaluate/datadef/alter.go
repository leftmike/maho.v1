package datadef

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type AlterAction interface {
	fmt.Stringer
	plan(ctx context.Context, pctx evaluate.PlanContext, tx sql.Transaction,
		tn sql.TableName) error
	execute(ctx context.Context, tx sql.Transaction, check bool) error
}

type AddForeignKey struct {
	ForeignKey
}

type AlterTable struct {
	Table   sql.TableName
	Actions []AlterAction
}

func (afk AddForeignKey) String() string {
	return fmt.Sprintf("ADD %s", afk.ForeignKey)
}

func (stmt *AlterTable) String() string {
	s := fmt.Sprintf("ALTER TABLE %s ", stmt.Table)
	for adx, act := range stmt.Actions {
		if adx > 0 {
			s += ", "
		}
		s += act.String()
	}
	return s
}

func (stmt *AlterTable) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction, cctx sql.CompileContext) (evaluate.Plan, error) {

	stmt.Table = pctx.ResolveTableName(stmt.Table)
	for _, act := range stmt.Actions {
		err := act.plan(ctx, pctx, tx, stmt.Table)
		if err != nil {
			return nil, err
		}
	}

	return stmt, nil
}

func (_ *AlterTable) Tag() string {
	return "ALTER TABLE"
}

func (stmt *AlterTable) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	for _, act := range stmt.Actions {
		err := tx.NextStmt(ctx)
		if err != nil {
			return -1, err
		}

		err = act.execute(ctx, tx, true)
		if err != nil {
			return -1, err
		}
	}

	return -1, nil
}
