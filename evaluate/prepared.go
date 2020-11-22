package evaluate

import (
	"context"
	"errors"
	"fmt"

	"github.com/leftmike/maho/flags"
	"github.com/leftmike/maho/sql"
)

type prepareContext struct {
	pctx   PlanContext
	params []*sql.Value
}

func (prep *prepareContext) GetFlag(f flags.Flag) bool {
	return prep.pctx.GetFlag(f)
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

func (prep *prepareContext) GetPreparedPlan(nam sql.Identifier) PreparedPlan {
	panic("unexpected, should never be called")
}

func PreparePlan(ctx context.Context, stmt Stmt, pctx PlanContext,
	tx sql.Transaction) (PreparedPlan, error) {

	prep := prepareContext{pctx: pctx}
	plan, err := stmt.Plan(ctx, &prep, tx)
	if err != nil {
		return nil, err
	}

	for num := range prep.params {
		if prep.params[num] == nil {
			return nil, fmt.Errorf("engine: prepare missing parameter $%d", num+1)
		}
	}

	return MakePreparedPlan(plan, prep.params), nil
}

type PreparedPlan interface {
	Plan
	SetParameters(params []sql.Value) error
}

type PreparedStmtPlan struct {
	plan   StmtPlan
	params []*sql.Value
}

type PreparedRowsPlan struct {
	plan   RowsPlan
	params []*sql.Value
}

func MakePreparedPlan(plan Plan, params []*sql.Value) PreparedPlan {
	if sp, ok := plan.(StmtPlan); ok {
		return &PreparedStmtPlan{
			plan:   sp,
			params: params,
		}
	} else if rp, ok := plan.(RowsPlan); ok {
		return &PreparedRowsPlan{
			plan:   rp,
			params: params,
		}
	} else {
		panic(fmt.Sprintf("expected a stmt or rows plan; got %#v", plan))
	}
}

func setParameters(prepParams []*sql.Value, params []sql.Value) error {
	if len(params) != len(prepParams) {
		return errors.New("engine: wrong number of parameters for prepared statement")
	}

	for pdx := range params {
		*prepParams[pdx] = params[pdx]
	}
	return nil
}

func (psp *PreparedStmtPlan) Tag() string {
	return psp.plan.Tag()
}

func (psp *PreparedStmtPlan) SetParameters(params []sql.Value) error {
	return setParameters(psp.params, params)
}

func (psp *PreparedStmtPlan) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	return psp.plan.Execute(ctx, tx)
}

func (prp *PreparedRowsPlan) Tag() string {
	return prp.plan.Tag()
}

func (prp *PreparedRowsPlan) SetParameters(params []sql.Value) error {
	return setParameters(prp.params, params)
}

func (prp *PreparedRowsPlan) Columns() []sql.Identifier {
	return prp.plan.Columns()
}

func (prp *PreparedRowsPlan) ColumnTypes() []sql.ColumnType {
	return prp.plan.ColumnTypes()
}

func (prp *PreparedRowsPlan) Rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	return prp.plan.Rows(ctx, tx)
}
