package expr

import (
	"fmt"
	"maho/sql"
	"maho/sql/expr"
	"math"
)

type CompileContext interface {
	Simplify() bool
}

func Compile(ctx CompileContext, e expr.Expr) (Expr, error) {
	switch e := e.(type) {
	case *expr.Literal:
		return &literal{e.Value}, nil
	case *expr.Unary:
		if e.Op == expr.NoOp {
			return Compile(ctx, e.Expr)
		}
		cf := opFuncs[e.Op]
		a1, err := Compile(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		return &call{cf, []Expr{a1}}, nil
	case *expr.Binary:
		cf := opFuncs[e.Op]
		a1, err := Compile(ctx, e.Left)
		if err != nil {
			return nil, err
		}
		a2, err := Compile(ctx, e.Right)
		if err != nil {
			return nil, err
		}
		return &call{cf, []Expr{a1, a2}}, nil
	case expr.Ref:
		panic("ref not handled yet")
	case *expr.Call:
		cf, ok := idFuncs[e.Name]
		if !ok {
			return nil, fmt.Errorf("engine: function \"%s\" not found", e.Name)
		}
		if len(e.Args) < int(cf.minArgs) {
			return nil, fmt.Errorf("engine: function \"%s\": minimum %d arguments got %d",
				e.Name, cf.minArgs, len(e.Args))
		}
		if len(e.Args) > int(cf.maxArgs) {
			return nil, fmt.Errorf("engine: function \"%s\": maximum %d arguments got %d",
				e.Name, cf.maxArgs, len(e.Args))
		}

		args := make([]Expr, len(e.Args))
		for i, a := range e.Args {
			var err error
			args[i], err = Compile(ctx, a)
			if err != nil {
				return nil, err
			}
		}
		return &call{cf, args}, nil
	default:
		panic("missing case for expr")
	}
}

type callFunc struct {
	fn      func(ctx EvalContext, args []sql.Value) (sql.Value, error)
	minArgs int16
	maxArgs int16
	name    string
}

var opFuncs = map[expr.Op]*callFunc{
	expr.AddOp:          {addCall, 2, 2, ""},
	expr.AndOp:          {andCall, 2, 2, ""},
	expr.BinaryAndOp:    {binaryAndCall, 2, 2, ""},
	expr.BinaryOrOp:     {binaryOrCall, 2, 2, ""},
	expr.ConcatOp:       {concatCall, 2, 2, ""},
	expr.DivideOp:       {divideCall, 2, 2, ""},
	expr.EqualOp:        {equalCall, 2, 2, ""},
	expr.GreaterEqualOp: {greaterEqualCall, 2, 2, ""},
	expr.GreaterThanOp:  {greaterThanCall, 2, 2, ""},
	expr.LessEqualOp:    {lessEqualCall, 2, 2, ""},
	expr.LessThanOp:     {lessThanCall, 2, 2, ""},
	expr.LShiftOp:       {lShiftCall, 2, 2, ""},
	expr.ModuloOp:       {moduloCall, 2, 2, ""},
	expr.MultiplyOp:     {multiplyCall, 2, 2, ""},
	expr.NegateOp:       {negateCall, 1, 1, ""},
	expr.NotEqualOp:     {notEqualCall, 2, 2, ""},
	expr.NotOp:          {notCall, 1, 1, ""},
	expr.OrOp:           {orCall, 2, 2, ""},
	expr.RShiftOp:       {rShiftCall, 2, 2, ""},
	expr.SubtractOp:     {subtractCall, 2, 2, ""},
}

var idFuncs = map[sql.Identifier]*callFunc{
	sql.ID("abs"):    {absCall, 1, 1, ""},
	sql.ID("concat"): {concatCall, 2, math.MaxInt16, ""},
}

func init() {
	for op, cf := range opFuncs {
		cf.name = fmt.Sprintf("\"%s\"", op)
	}

	for id, cf := range idFuncs {
		cf.name = id.String()
	}
}
