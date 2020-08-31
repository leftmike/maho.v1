package expr

import (
	"context"
	"fmt"
	"math"

	"github.com/leftmike/maho/evaluate"
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

func CompileRef(idx int) sql.CExpr {
	return colIndex(idx)
}

func Compile(ctx context.Context, tx sql.Transaction, cctx CompileContext, e Expr) (sql.CExpr,
	error) {

	return compile(ctx, tx, cctx, e, false)
}

func CompileAggregator(ctx context.Context, tx sql.Transaction, cctx CompileContext,
	e Expr) (sql.CExpr, error) {

	return compile(ctx, tx, cctx, e, true)
}

func CompileExpr(e Expr) (sql.CExpr, error) {
	return compile(nil, nil, nil, e, false)
}

func compile(ctx context.Context, tx sql.Transaction, cctx CompileContext, e Expr,
	agg bool) (sql.CExpr, error) {

	if agg {
		idx, ok := cctx.(AggregatorContext).MaybeRefExpr(e)
		if ok {
			return colIndex(idx), nil
		}
	}
	switch e := e.(type) {
	case *Literal:
		return e, nil
	case *Unary:
		if e.Op == NoOp {
			return compile(ctx, tx, cctx, e.Expr, agg)
		}
		cf := opFuncs[e.Op]
		a1, err := compile(ctx, tx, cctx, e.Expr, agg)
		if err != nil {
			return nil, err
		}
		return &call{cf, []sql.CExpr{a1}}, nil
	case *Binary:
		cf := opFuncs[e.Op]
		a1, err := compile(ctx, tx, cctx, e.Left, agg)
		if err != nil {
			return nil, err
		}
		a2, err := compile(ctx, tx, cctx, e.Right, agg)
		if err != nil {
			return nil, err
		}
		return &call{cf, []sql.CExpr{a1, a2}}, nil
	case Ref:
		if cctx == nil {
			return nil, fmt.Errorf("engine: %s not found", e)
		}
		idx, err := cctx.CompileRef(e)
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
				return colIndex(cctx.(AggregatorContext).CompileAggregator(e, cf.makeAggregator)),
					nil
			} else {
				return nil, &ContextError{e.Name}
			}
		}

		args := make([]sql.CExpr, len(e.Args))
		for i, a := range e.Args {
			var err error
			args[i], err = compile(ctx, tx, cctx, a, agg)
			if err != nil {
				return nil, err
			}
		}
		return &call{cf, args}, nil
	case Stmt:
		if tx == nil {
			return nil, fmt.Errorf("engine: expression statements not allowed here: %s", e.Stmt)
		}

		plan, err := e.Stmt.Plan(ctx, tx)
		if err != nil {
			return nil, err
		}

		rowsPlan, ok := plan.(evaluate.RowsPlan)
		if !ok {
			return nil, fmt.Errorf("engine: expected rows: %s", e.Stmt)
		}
		rows, err := rowsPlan.Rows(ctx, tx)
		if err != nil {
			return nil, err
		}
		return &rowsExpr{rows: rows}, nil
	default:
		panic("missing case for expr")
	}
}

type callFunc struct {
	fn             func(ectx sql.EvalContext, args []sql.Value) (sql.Value, error)
	minArgs        int16
	maxArgs        int16
	name           string
	handleNull     bool
	makeAggregator MakeAggregator
}

var (
	opFuncs = map[Op]*callFunc{
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

	idFuncs = map[sql.Identifier]*callFunc{
		// Scalar functions
		sql.ID("abs"): {fn: absCall, minArgs: 1, maxArgs: 1},
		sql.ID("concat"): {fn: concatCall, minArgs: 2, maxArgs: math.MaxInt16,
			handleNull: true},
		sql.ID("unique_rowid"): {fn: uniqueRowIDCall, minArgs: 0, maxArgs: 0},

		// Aggregate functions
		sql.ID("avg"):       {minArgs: 1, maxArgs: 1, makeAggregator: makeAvgAggregator},
		sql.ID("count"):     {minArgs: 1, maxArgs: 1, makeAggregator: makeCountAggregator},
		sql.ID("count_all"): {minArgs: 0, maxArgs: 0, makeAggregator: makeCountAllAggregator},
		sql.ID("max"):       {minArgs: 1, maxArgs: 1, makeAggregator: makeMaxAggregator},
		sql.ID("min"):       {minArgs: 1, maxArgs: 1, makeAggregator: makeMinAggregator},
		sql.ID("sum"):       {minArgs: 1, maxArgs: 1, makeAggregator: makeSumAggregator},
	}

	funcs = map[string]*callFunc{}
)

func init() {
	for op, cf := range opFuncs {
		if op == NegateOp {
			cf.name = "negate"
		} else {
			cf.name = fmt.Sprintf("\"%s\"", op)
		}

		if op == NegateOp || op == NotOp {
			if cf.minArgs != 1 || cf.maxArgs != 1 {
				panic(fmt.Sprintf("opFuncs[%s]: minArgs != 1 || maxArgs != 1", op))
			}
		} else {
			if cf.minArgs != 2 || cf.maxArgs != 2 {
				panic(fmt.Sprintf("opFuncs[%s]: minArgs != 2 || maxArgs != 2", op))
			}
		}

		if _, ok := funcs[cf.name]; ok {
			panic(fmt.Sprintf("duplicate function name: %s", cf.name))
		}
		funcs[cf.name] = cf
	}

	for id, cf := range idFuncs {
		cf.name = id.String()
		if cf.minArgs < 0 || cf.maxArgs < cf.minArgs {
			panic(fmt.Sprintf("opFuncs[%s]: minArgs < 0 || maxArgs < minArgs", id))
		}

		if _, ok := funcs[cf.name]; ok {
			panic(fmt.Sprintf("duplicate function name: %s", cf.name))
		}
		funcs[cf.name] = cf
	}
}
