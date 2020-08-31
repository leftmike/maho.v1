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
	Eval(ctx context.Context, ectx EvalContext) (Value, error)
}
