package misc

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type Prepare struct {
	Name sql.Identifier
	Stmt evaluate.Stmt
	prep evaluate.PreparedPlan
}

func (stmt *Prepare) String() string {
	return fmt.Sprintf("PREPARE %s AS %s", stmt.Name, stmt.Stmt)
}

type prepareContext struct {
	pctx   evaluate.PlanContext
	params []*sql.Value
}

func (prep *prepareContext) ResolveTableName(tn sql.TableName) sql.TableName {
	return prep.pctx.ResolveTableName(tn)
}

func (prep *prepareContext) ResolveSchemaName(sn sql.SchemaName) sql.SchemaName {
	return prep.pctx.ResolveSchemaName(sn)
}

func (prep *prepareContext) PlanParameter(num int) (*sql.Value, error) {
	if num > len(prep.params) {
		nparams := make([]*sql.Value, num)
		if prep.params != nil {
			copy(nparams, prep.params)
		}
		prep.params = nparams
	}

	ptr := prep.params[num-1]
	if ptr == nil {
		var val sql.Value
		ptr = &val
		prep.params[num-1] = ptr
	}
	return ptr, nil
}

func (prep *prepareContext) GetPreparedPlan(nam sql.Identifier) evaluate.PreparedPlan {
	panic("unexpected, should never be called")
}

func (stmt *Prepare) PreparePlan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction) (evaluate.PreparedPlan, error) {

	prep := prepareContext{pctx: pctx}
	plan, err := stmt.Stmt.Plan(ctx, &prep, tx)
	if err != nil {
		return nil, err
	}

	for num := range prep.params {
		if prep.params[num] == nil {
			return nil, fmt.Errorf("engine: prepare missing parameter $%d", num+1)
		}
	}

	return evaluate.MakePreparedPlan(plan, prep.params), nil
}

func (stmt *Prepare) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction) (evaluate.Plan, error) {

	var err error
	stmt.prep, err = stmt.PreparePlan(ctx, pctx, tx)
	if err != nil {
		return nil, err
	}

	return stmt, nil
}

func (stmt *Prepare) Planned() {}

func (stmt *Prepare) Command(ctx context.Context, ses *evaluate.Session, e sql.Engine) error {
	return ses.SetPreparedPlan(stmt.Name, stmt.prep)
}
