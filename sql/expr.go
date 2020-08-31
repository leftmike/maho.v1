package sql

import (
	"context"
	"fmt"
)

type EvalContext interface {
	EvalRef(idx int) Value
}

type CExpr interface {
	fmt.Stringer
	Eval(ctx context.Context, tx Transaction, ectx EvalContext) (Value, error)
}
