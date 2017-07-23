package expr

import (
	"fmt"
	"maho/sql"
	"maho/sql/expr"
)

type EvalContext interface {
}

type Expr interface {
	fmt.Stringer
	Eval(ctx EvalContext) (sql.Value, error)
}

type literal expr.Literal

func (l *literal) String() string {
	return sql.Format(l.Value)
}

func (l *literal) Eval(ctx EvalContext) (sql.Value, error) {
	return l.Value, nil
}

type call struct {
	call *callFunc
	args []Expr
}

func (c *call) String() string {
	s := fmt.Sprintf("%s(", c.call.name)
	for i, a := range c.args {
		if i > 0 {
			s += ", "
		}
		s += a.String()
	}
	s += ")"
	return s
}

func (c *call) Eval(ctx EvalContext) (sql.Value, error) {
	args := make([]sql.Value, len(c.args))
	for i, a := range c.args {
		var err error
		args[i], err = a.Eval(ctx)
		if err != nil {
			return nil, err
		}
	}
	return c.call.fn(ctx, args)
}

func numFunc(a0 sql.Value, a1 sql.Value, ifn func(i0, i1 int64) sql.Value,
	ffn func(f0, f1 float64) sql.Value) (sql.Value, error) {
	if a0 == nil || a1 == nil {
		return nil, nil
	}
	switch a0 := a0.(type) {
	case float64:
		switch a1 := a1.(type) {
		case float64:
			return ffn(a0, a1), nil
		case int64:
			return ffn(a0, float64(a1)), nil
		}
	case int64:
		switch a1 := a1.(type) {
		case float64:
			return ffn(float64(a0), a1), nil
		case int64:
			return ifn(a0, a1), nil
		}
	}

	// XXX: better error message and panic if not expected type in sql.Value
	return nil, fmt.Errorf("engine: want number")
}

func intFunc(a0 sql.Value, a1 sql.Value, ifn func(i0, i1 int64) sql.Value) (sql.Value, error) {
	if a0 == nil || a1 == nil {
		return nil, nil
	}
	if a0, ok := a0.(int64); ok {
		if a1, ok := a1.(int64); ok {
			return ifn(a0, a1), nil
		}
	}
	return nil, fmt.Errorf("engine: want integer")
}

func shiftFunc(a0 sql.Value, a1 sql.Value, ifn func(i0 int64, i1 uint64) sql.Value) (sql.Value,
	error) {
	if a0 == nil || a1 == nil {
		return nil, nil
	}
	if a0, ok := a0.(int64); ok {
		if a1, ok := a1.(int64); ok {
			if a1 < 0 {
				return nil, fmt.Errorf("engine: want non-negative integer")
			}
			return ifn(a0, uint64(a1)), nil
		}
	}
	return nil, fmt.Errorf("engine: want integer")
}

func addCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return numFunc(args[0], args[1],
		func(i0, i1 int64) sql.Value {
			return i0 + i1
		},
		func(f0, f1 float64) sql.Value {
			return f0 + f1
		})
}

func andCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	if a0, ok := args[0].(bool); ok {
		if a1, ok := args[1].(bool); ok {
			return a0 && a1, nil
		}
	}
	return nil, fmt.Errorf("engine: want boolean")
}

func binaryAndCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return intFunc(args[0], args[1],
		func(i0, i1 int64) sql.Value {
			return i0 & i1
		})
}

func binaryOrCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return intFunc(args[0], args[1],
		func(i0, i1 int64) sql.Value {
			return i0 | i1
		})
}

func concatCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	// XXX
	return nil, nil
}

func divideCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return numFunc(args[0], args[1],
		func(i0, i1 int64) sql.Value {
			return i0 / i1
		},
		func(f0, f1 float64) sql.Value {
			return f0 / f1
		})
}

func equalCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	// XXX
	return nil, nil
}

func greaterEqualCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	// XXX
	return nil, nil
}

func greaterThanCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	// XXX
	return nil, nil
}

func lessEqualCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	// XXX
	return nil, nil
}

func lessThanCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	// XXX
	return nil, nil
}

func lShiftCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return shiftFunc(args[0], args[1],
		func(i0 int64, i1 uint64) sql.Value {
			return i0 << i1
		})
}

func moduloCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return intFunc(args[0], args[1],
		func(i0, i1 int64) sql.Value {
			return i0 % i1
		})
}

func multiplyCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return numFunc(args[0], args[1],
		func(i0, i1 int64) sql.Value {
			return i0 * i1
		},
		func(f0, f1 float64) sql.Value {
			return f0 * f1
		})
}

func negateCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	if args[0] == nil {
		return nil, nil
	}
	switch a0 := args[0].(type) {
	case float64:
		return -a0, nil
	case int64:
		return -a0, nil
	}
	return nil, fmt.Errorf("engine: want number")
}

func notEqualCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	// XXX
	return nil, nil
}

func notCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	if args[0] == nil {
		return nil, nil
	}
	if a0, ok := args[0].(bool); ok {
		return a0 == false, nil
	}
	return nil, fmt.Errorf("engine: want boolean")
}

func orCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}

	if a0, ok := args[0].(bool); ok {
		if a1, ok := args[1].(bool); ok {
			return a0 || a1, nil
		}
	}
	return nil, fmt.Errorf("engine: want boolean")
}

func rShiftCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return shiftFunc(args[0], args[1],
		func(i0 int64, i1 uint64) sql.Value {
			return i0 >> i1
		})
}

func subtractCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return numFunc(args[0], args[1],
		func(i0, i1 int64) sql.Value {
			return i0 - i1
		},
		func(f0, f1 float64) sql.Value {
			return f0 - f1
		})
}

func absCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	if args[0] == nil {
		return nil, nil
	}
	switch a0 := args[0].(type) {
	case float64:
		if a0 < 0 {
			return -a0, nil
		}
		return a0, nil
	case int64:
		if a0 < 0 {
			return -a0, nil
		}
		return a0, nil
	}
	return nil, fmt.Errorf("engine: want number")
}
