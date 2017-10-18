package expr

import (
	"fmt"
	"maho/sql"
	"math"
)

type CompileContext interface {
	CompileRef(r Ref) (int, error)
}

func Compile(ctx CompileContext, e Expr) (CExpr, error) {
	switch e := e.(type) {
	case *Literal:
		return e, nil
	case *Unary:
		if e.Op == NoOp {
			return Compile(ctx, e.Expr)
		}
		cf := opFuncs[e.Op]
		a1, err := Compile(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		return &call{cf, []CExpr{a1}}, nil
	case *Binary:
		cf := opFuncs[e.Op]
		a1, err := Compile(ctx, e.Left)
		if err != nil {
			return nil, err
		}
		a2, err := Compile(ctx, e.Right)
		if err != nil {
			return nil, err
		}
		return &call{cf, []CExpr{a1, a2}}, nil
	case Ref:
		if ctx == nil {
			return nil, fmt.Errorf("reference %s not found", e)
		}
		idx, err := ctx.CompileRef(e)
		if err != nil {
			return nil, err
		}
		return colIndex(idx), nil
	case *Call:
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

		args := make([]CExpr, len(e.Args))
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

var opFuncs = map[Op]*callFunc{
	AddOp:          {addCall, 2, 2, ""},
	AndOp:          {andCall, 2, 2, ""},
	BinaryAndOp:    {binaryAndCall, 2, 2, ""},
	BinaryOrOp:     {binaryOrCall, 2, 2, ""},
	ConcatOp:       {concatCall, 2, 2, ""},
	DivideOp:       {divideCall, 2, 2, ""},
	EqualOp:        {equalCall, 2, 2, ""},
	GreaterEqualOp: {greaterEqualCall, 2, 2, ""},
	GreaterThanOp:  {greaterThanCall, 2, 2, ""},
	LessEqualOp:    {lessEqualCall, 2, 2, ""},
	LessThanOp:     {lessThanCall, 2, 2, ""},
	LShiftOp:       {lShiftCall, 2, 2, ""},
	ModuloOp:       {moduloCall, 2, 2, ""},
	MultiplyOp:     {multiplyCall, 2, 2, ""},
	NegateOp:       {negateCall, 1, 1, ""},
	NotEqualOp:     {notEqualCall, 2, 2, ""},
	NotOp:          {notCall, 1, 1, ""},
	OrOp:           {orCall, 2, 2, ""},
	RShiftOp:       {rShiftCall, 2, 2, ""},
	SubtractOp:     {subtractCall, 2, 2, ""},
}

var idFuncs = map[sql.Identifier]*callFunc{
	sql.ID("abs"):    {absCall, 1, 1, ""},
	sql.ID("concat"): {concatCall, 2, math.MaxInt16, ""},
}

func init() {
	for op, cf := range opFuncs {
		cf.name = fmt.Sprintf("\"%s\"", op)
		if op == NegateOp || op == NotOp {
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
