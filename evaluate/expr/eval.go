package expr

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type EvalContext interface {
	EvalRef(idx int) sql.Value
}

type CExpr interface {
	fmt.Stringer
	Eval(ctx context.Context, etx EvalContext) (sql.Value, error)
}

func (l *Literal) Eval(ctx context.Context, etx EvalContext) (sql.Value, error) {
	return l.Value, nil
}

type colIndex int

func (ci colIndex) String() string {
	return fmt.Sprintf("row[%d]", ci)
}

func (ci colIndex) Eval(ctx context.Context, etx EvalContext) (sql.Value, error) {
	return etx.EvalRef(int(ci)), nil
}

func ColumnIndex(ce CExpr) (int, bool) {
	if ci, ok := ce.(colIndex); ok {
		return int(ci), true
	}
	return 0, false
}

type rowsExpr struct {
	rows  engine.Rows
	value sql.Value
	err   error
	done  bool
}

func (re *rowsExpr) String() string {
	return fmt.Sprintf("rows: %#v", re)
}

func (re *rowsExpr) eval(ctx context.Context, etx EvalContext) (sql.Value, error) {
	if len(re.rows.Columns()) != 1 {
		return nil, errors.New("engine: expected one column for scalar subquery")
	}
	dest := []sql.Value{nil}
	err := re.rows.Next(ctx, dest)
	if err != nil {
		return nil, err
	}
	err = re.rows.Next(ctx, []sql.Value{nil})
	if err == nil {
		return nil, errors.New("engine: expected one row for scalar subquery")
	} else if err != io.EOF {
		return nil, err
	}
	return dest[0], nil
}

func (re *rowsExpr) Eval(ctx context.Context, etx EvalContext) (sql.Value, error) {
	if !re.done {
		re.done = true
		re.value, re.err = re.eval(ctx, etx)
	}
	return re.value, re.err
}

type call struct {
	call *callFunc
	args []CExpr
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

func (c *call) Eval(ctx context.Context, etx EvalContext) (sql.Value, error) {
	args := make([]sql.Value, len(c.args))
	for i, a := range c.args {
		var err error
		args[i], err = a.Eval(ctx, etx)
		if err != nil {
			return nil, err
		} else if args[i] == nil && !c.call.handleNull {
			return nil, nil
		}
	}
	return c.call.fn(etx, args)
}

func numFunc(a0 sql.Value, a1 sql.Value, ifn func(i0, i1 sql.Int64Value) sql.Value,
	ffn func(f0, f1 sql.Float64Value) sql.Value) (sql.Value, error) {

	switch a0 := a0.(type) {
	case sql.Float64Value:
		switch a1 := a1.(type) {
		case sql.Float64Value:
			return ffn(a0, a1), nil
		case sql.Int64Value:
			return ffn(a0, sql.Float64Value(a1)), nil
		}
	case sql.Int64Value:
		switch a1 := a1.(type) {
		case sql.Float64Value:
			return ffn(sql.Float64Value(a0), a1), nil
		case sql.Int64Value:
			return ifn(a0, a1), nil
		}
	default:
		return nil, fmt.Errorf("engine: want number got %v", a0)
	}
	return nil, fmt.Errorf("engine: want number got %v", a1)
}

func intFunc(a0 sql.Value, a1 sql.Value, ifn func(i0, i1 sql.Int64Value) sql.Value) (sql.Value,
	error) {

	if a0, ok := a0.(sql.Int64Value); ok {
		if a1, ok := a1.(sql.Int64Value); ok {
			return ifn(a0, a1), nil
		}
		return nil, fmt.Errorf("engine: want integer got %v", a1)
	}
	return nil, fmt.Errorf("engine: want integer got %v", a0)
}

func shiftFunc(a0 sql.Value, a1 sql.Value,
	ifn func(i0 sql.Int64Value, i1 uint64) sql.Value) (sql.Value, error) {

	if a0, ok := a0.(sql.Int64Value); ok {
		if a1, ok := a1.(sql.Int64Value); ok {
			if a1 < 0 {
				return nil, fmt.Errorf("engine: want non-negative integer got %v", a1)
			}
			return ifn(a0, uint64(a1)), nil
		}
		return nil, fmt.Errorf("engine: want integer got %v", a1)
	}
	return nil, fmt.Errorf("engine: want integer got %v", a0)
}

func addCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return numFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 + i1
		},
		func(f0, f1 sql.Float64Value) sql.Value {
			return f0 + f1
		})
}

func andCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	if a0, ok := args[0].(sql.BoolValue); ok {
		if a1, ok := args[1].(sql.BoolValue); ok {
			return a0 && a1, nil
		}
		return nil, fmt.Errorf("engine: want boolean got %v", args[1])
	}
	return nil, fmt.Errorf("engine: want boolean got %v", args[0])
}

func binaryAndCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return intFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 & i1
		})
}

func binaryOrCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return intFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 | i1
		})
}

func concatCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	s := ""
	for _, a := range args {
		if a == nil {
			continue
		}
		switch v := a.(type) {
		case sql.BoolValue:
			if v {
				s += sql.TrueString
			} else {
				s += sql.FalseString
			}
		case sql.StringValue:
			s += string(v)
		case sql.BytesValue:
			s += fmt.Sprintf("%v", v)
		case sql.Float64Value:
			s += fmt.Sprintf("%v", v)
		case sql.Int64Value:
			s += fmt.Sprintf("%v", v)
		default:
			panic("unexpected sql.Value")
		}
	}
	return sql.StringValue(s), nil
}

func divideCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return numFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 / i1
		},
		func(f0, f1 sql.Float64Value) sql.Value {
			return f0 / f1
		})
}

func equalCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	cmp, err := args[0].Compare(args[1])
	if err != nil {
		return nil, err
	}
	return sql.BoolValue(cmp == 0), nil
}

func greaterEqualCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	cmp, err := args[0].Compare(args[1])
	if err != nil {
		return nil, err
	}
	return sql.BoolValue(cmp >= 0), nil
}

func greaterThanCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	cmp, err := args[0].Compare(args[1])
	if err != nil {
		return nil, err
	}
	return sql.BoolValue(cmp > 0), nil
}

func lessEqualCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	cmp, err := args[0].Compare(args[1])
	if err != nil {
		return nil, err
	}
	return sql.BoolValue(cmp <= 0), nil
}

func lessThanCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	cmp, err := args[0].Compare(args[1])
	if err != nil {
		return nil, err
	}
	return sql.BoolValue(cmp < 0), nil
}

func lShiftCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return shiftFunc(args[0], args[1],
		func(i0 sql.Int64Value, i1 uint64) sql.Value {
			return i0 << i1
		})
}

func moduloCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return intFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 % i1
		})
}

func multiplyCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return numFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 * i1
		},
		func(f0, f1 sql.Float64Value) sql.Value {
			return f0 * f1
		})
}

func negateCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {

	switch a0 := args[0].(type) {
	case sql.Float64Value:
		return -a0, nil
	case sql.Int64Value:
		return -a0, nil
	}
	return nil, fmt.Errorf("engine: want number got %v", args[0])
}

func notEqualCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	cmp, err := args[0].Compare(args[1])
	if err != nil {
		return nil, err
	}
	return sql.BoolValue(cmp != 0), nil
}

func notCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {

	if a0, ok := args[0].(sql.BoolValue); ok {
		return sql.BoolValue(a0 == false), nil
	}
	return nil, fmt.Errorf("engine: want boolean got %v", args[0])
}

func orCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {

	if a0, ok := args[0].(sql.BoolValue); ok {
		if a1, ok := args[1].(sql.BoolValue); ok {
			return a0 || a1, nil
		}
	}
	return nil, fmt.Errorf("engine: want boolean got %v", args[0])
}

func rShiftCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return shiftFunc(args[0], args[1],
		func(i0 sql.Int64Value, i1 uint64) sql.Value {
			return i0 >> i1
		})
}

func subtractCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	return numFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 - i1
		},
		func(f0, f1 sql.Float64Value) sql.Value {
			return f0 - f1
		})
}

func absCall(ctx EvalContext, args []sql.Value) (sql.Value, error) {
	switch a0 := args[0].(type) {
	case sql.Float64Value:
		if a0 < 0 {
			return -a0, nil
		}
		return a0, nil
	case sql.Int64Value:
		if a0 < 0 {
			return -a0, nil
		}
		return a0, nil
	}
	return nil, fmt.Errorf("engine: want number got %v", args[0])
}
