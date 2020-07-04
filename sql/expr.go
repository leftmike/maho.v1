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
	Eval(ctx context.Context, etx EvalContext) (Value, error)
}
