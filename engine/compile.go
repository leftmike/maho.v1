package engine

import (
	"fmt"
	"maho/sql"
	"math"
)

type CompileContext interface {
	Simplify() bool
}

func Compile(ctx CompileContext, e sql.Expr) (Expr, error) {
	switch e := e.(type) {
	case *sql.Literal:
		return &literal{e.Value}, nil
	case *sql.Unary:
		if e.Op == sql.NoOp {
			return Compile(ctx, e.Expr)
		}
		cf := opFuncs[e.Op]
		a1, err := Compile(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		return &call{cf, []Expr{a1}}, nil
	case *sql.Binary:
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
	case sql.Ref:
		panic("ref not handled yet")
	case *sql.Call:
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

var opFuncs = map[sql.Op]*callFunc{
	sql.AddOp:          {addCall, 2, 2, ""},
	sql.AndOp:          {andCall, 2, 2, ""},
	sql.BinaryAndOp:    {binaryAndCall, 2, 2, ""},
	sql.BinaryOrOp:     {binaryOrCall, 2, 2, ""},
	sql.ConcatOp:       {concatCall, 2, 2, ""},
	sql.DivideOp:       {divideCall, 2, 2, ""},
	sql.EqualOp:        {equalCall, 2, 2, ""},
	sql.GreaterEqualOp: {greaterEqualCall, 2, 2, ""},
	sql.GreaterThanOp:  {greaterThanCall, 2, 2, ""},
	sql.LessEqualOp:    {lessEqualCall, 2, 2, ""},
	sql.LessThanOp:     {lessThanCall, 2, 2, ""},
	sql.LShiftOp:       {lShiftCall, 2, 2, ""},
	sql.ModuloOp:       {moduloCall, 2, 2, ""},
	sql.MultiplyOp:     {multiplyCall, 2, 2, ""},
	sql.NegateOp:       {negateCall, 1, 1, ""},
	sql.NotEqualOp:     {notEqualCall, 2, 2, ""},
	sql.NotOp:          {notCall, 1, 1, ""},
	sql.OrOp:           {orCall, 2, 2, ""},
	sql.RShiftOp:       {rShiftCall, 2, 2, ""},
	sql.SubtractOp:     {subtractCall, 2, 2, ""},
}

var idFuncs = map[sql.Identifier]*callFunc{
	sql.ID("abs"):    {absCall, 1, 1, ""},
	sql.ID("concat"): {concatCall, 2, math.MaxInt16, ""},
}

func init() {
	for op, cf := range opFuncs {
		cf.name = fmt.Sprintf("\"%s\"", op)
		if op == sql.NegateOp || op == sql.NotOp {
			if cf.minArgs != 1 || cf.maxArgs != 1 {
				panic(fmt.Sprintf("opFuncs[%s]: minArgs != 1 || maxArgs != 1", op))
			}
		} else {
			if cf.minArgs != 2 || cf.maxArgs != 2 {
				panic(fmt.Sprintf("opFuncs[%s]: minArgs != 2 || maxArgs != 2", op))
			}
		}
	}

	for id, cf := range idFuncs {
		cf.name = id.String()
		if cf.minArgs < 0 || cf.maxArgs < cf.minArgs {
			panic(fmt.Sprintf("opFuncs[%s]: minArgs < 0 || maxArgs < minArgs", id))
		}
	}
}
