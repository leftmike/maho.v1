package evaluate

import (
	"context"
	"errors"
	"fmt"

	"github.com/leftmike/maho/sql"
)

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

func (psp *PreparedStmtPlan) Planned() {}

func (psp *PreparedStmtPlan) SetParameters(params []sql.Value) error {
	return setParameters(psp.params, params)
}

func (psp *PreparedStmtPlan) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	return psp.plan.Execute(ctx, tx)
}

func (prp *PreparedRowsPlan) Planned() {}

func (prp *PreparedRowsPlan) SetParameters(params []sql.Value) error {
	return setParameters(prp.params, params)
}

func (prp *PreparedRowsPlan) Columns() []sql.Identifier {
	return prp.plan.Columns()
}

func (prp *PreparedRowsPlan) Rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	return prp.plan.Rows(ctx, tx)
}
