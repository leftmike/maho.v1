package sql

import (
	"context"
	"fmt"
)

type CompileContext interface {
	CompileRef(r []Identifier) (int, int, ColumnType, error)
}

type EvalContext interface {
	EvalRef(idx, nest int) Value
}

type CExpr interface {
	fmt.Stringer
	Eval(ctx context.Context, tx Transaction, ectx EvalContext) (Value, error)
}
