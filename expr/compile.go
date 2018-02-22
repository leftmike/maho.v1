package expr

import (
	"fmt"
	"math"

	"github.com/leftmike/maho/sql"
)

type CompileContext interface {
	CompileRef(r Ref) (int, error)
}

type AggregatorContext interface {
	MaybeRefExpr(e Expr) (int, bool)
	CompileAggregator(c *Call, maker MakeAggregator) int
}

type ContextError struct {
	name sql.Identifier
}

func (e *ContextError) Error() string {
	return fmt.Sprintf("engine: aggregate function \"%s\" used in scalar context", e.name)
}

func CompileRef(idx int) CExpr {
	return colIndex(idx)
}

func Compile(ctx CompileContext, e Expr, agg bool) (CExpr, error) {
	if agg {
		idx, ok := ctx.(AggregatorContext).MaybeRefExpr(e)
		if ok {
			return colIndex(idx), nil
		}
	}
	switch e := e.(type) {
	case *Literal:
		return e, nil
	case *Unary:
		if e.Op == NoOp {
			return Compile(ctx, e.Expr, agg)
		}
		cf := opFuncs[e.Op]
		a1, err := Compile(ctx, e.Expr, agg)
		if err != nil {
			return nil, err
		}
		return &call{cf, []CExpr{a1}}, nil
	case *Binary:
		cf := opFuncs[e.Op]
		a1, err := Compile(ctx, e.Left, agg)
		if err != nil {
			return nil, err
		}
		a2, err := Compile(ctx, e.Right, agg)
		if err != nil {
			return nil, err
		}
		return &call{cf, []CExpr{a1, a2}}, nil
	case Ref:
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
		if cf.makeAggregator != nil {
			if agg {
				return colIndex(ctx.(AggregatorContext).CompileAggregator(e, cf.makeAggregator)),
					nil
			} else {
				return nil, &ContextError{e.Name}
			}
		}

		args := make([]CExpr, len(e.Args))
		for i, a := range e.Args {
			var err error
			args[i], err = Compile(ctx, a, agg)
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
	fn             func(ctx EvalContext, args []sql.Value) (sql.Value, error)
	minArgs        int16
	maxArgs        int16
	name           string
	handleNull     bool
	makeAggregator MakeAggregator
}

var opFuncs = map[Op]*callFunc{
	AddOp:          {fn: addCall, minArgs: 2, maxArgs: 2},
	AndOp:          {fn: andCall, minArgs: 2, maxArgs: 2},
	BinaryAndOp:    {fn: binaryAndCall, minArgs: 2, maxArgs: 2},
	BinaryOrOp:     {fn: binaryOrCall, minArgs: 2, maxArgs: 2},
	ConcatOp:       {fn: concatCall, minArgs: 2, maxArgs: 2, handleNull: true},
	DivideOp:       {fn: divideCall, minArgs: 2, maxArgs: 2},
	EqualOp:        {fn: equalCall, minArgs: 2, maxArgs: 2},
	GreaterEqualOp: {fn: greaterEqualCall, minArgs: 2, maxArgs: 2},
	GreaterThanOp:  {fn: greaterThanCall, minArgs: 2, maxArgs: 2},
	LessEqualOp:    {fn: lessEqualCall, minArgs: 2, maxArgs: 2},
	LessThanOp:     {fn: lessThanCall, minArgs: 2, maxArgs: 2},
	LShiftOp:       {fn: lShiftCall, minArgs: 2, maxArgs: 2},
	ModuloOp:       {fn: moduloCall, minArgs: 2, maxArgs: 2},
	MultiplyOp:     {fn: multiplyCall, minArgs: 2, maxArgs: 2},
	NegateOp:       {fn: negateCall, minArgs: 1, maxArgs: 1},
	NotEqualOp:     {fn: notEqualCall, minArgs: 2, maxArgs: 2},
	NotOp:          {fn: notCall, minArgs: 1, maxArgs: 1},
	OrOp:           {fn: orCall, minArgs: 2, maxArgs: 2},
	RShiftOp:       {fn: rShiftCall, minArgs: 2, maxArgs: 2},
	SubtractOp:     {fn: subtractCall, minArgs: 2, maxArgs: 2},
}

var idFuncs = map[sql.Identifier]*callFunc{
	// Scalar functions
	sql.ID("abs"):    {fn: absCall, minArgs: 1, maxArgs: 1},
	sql.ID("concat"): {fn: concatCall, minArgs: 2, maxArgs: math.MaxInt16, handleNull: true},

	// Aggregate functions
	sql.ID("avg"):       {minArgs: 1, maxArgs: 1, makeAggregator: makeAvgAggregator},
	sql.ID("count"):     {minArgs: 1, maxArgs: 1, makeAggregator: makeCountAggregator},
	sql.ID("count_all"): {minArgs: 0, maxArgs: 0, makeAggregator: makeCountAllAggregator},
	sql.ID("max"):       {minArgs: 1, maxArgs: 1, makeAggregator: makeMaxAggregator},
	sql.ID("min"):       {minArgs: 1, maxArgs: 1, makeAggregator: makeMinAggregator},
	sql.ID("sum"):       {minArgs: 1, maxArgs: 1, makeAggregator: makeSumAggregator},
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
