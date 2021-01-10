package expr

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
)

type aggregatorContext interface {
	MaybeRefExpr(e Expr) (int, sql.ColumnType, bool)
	CompileAggregator(maker MakeAggregator, args []sql.CExpr) int
	ArgContext() sql.CompileContext
}

type ContextError struct {
	name sql.Identifier
}

func (e *ContextError) Error() string {
	return fmt.Sprintf("engine: aggregate function \"%s\" used in scalar context", e.name)
}

func Compile(ctx context.Context, pctx evaluate.PlanContext, tx sql.Transaction,
	cctx sql.CompileContext, e Expr) (sql.CExpr, sql.ColumnType, error) {

	return compile(ctx, pctx, tx, cctx, e, false)
}

func CompileAggregator(ctx context.Context, pctx evaluate.PlanContext, tx sql.Transaction,
	cctx sql.CompileContext, e Expr) (sql.CExpr, sql.ColumnType, error) {

	return compile(ctx, pctx, tx, cctx, e, true)
}

func compile(ctx context.Context, pctx evaluate.PlanContext, tx sql.Transaction,
	cctx sql.CompileContext, e Expr, agg bool) (sql.CExpr, sql.ColumnType, error) {

	var ct sql.ColumnType

	if agg {
		idx, ct, ok := cctx.(aggregatorContext).MaybeRefExpr(e)
		if ok {
			return &colRef{idx: idx}, ct, nil
		}
	}
	switch e := e.(type) {
	case *Literal:
		if e.Value == nil {
			return e, sql.ColumnType{Type: sql.UnknownType}, nil
		}

		switch e.Value.(type) {
		case sql.BoolValue:
			return e, sql.ColumnType{Type: sql.BooleanType, NotNull: true}, nil
		case sql.Float64Value:
			return e, sql.ColumnType{Type: sql.FloatType, Size: 8, NotNull: true}, nil
		case sql.Int64Value:
			return e, sql.ColumnType{Type: sql.IntegerType, Size: 8, NotNull: true}, nil
		case sql.StringValue:
			return e, sql.ColumnType{Type: sql.StringType, NotNull: true}, nil
		case sql.BytesValue:
			return e, sql.ColumnType{Type: sql.BytesType, NotNull: true}, nil
		default:
			panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", e.Value, e.Value))
		}
	case *Unary:
		if e.Op == NoOp {
			return compile(ctx, pctx, tx, cctx, e.Expr, agg)
		}
		cf := opFuncs[e.Op]
		a1, ct1, err := compile(ctx, pctx, tx, cctx, e.Expr, agg)
		if err != nil {
			return nil, ct, err
		}
		if cf.tfn != nil {
			ct = cf.tfn([]sql.ColumnType{ct1})
		} else {
			ct = cf.typ
		}
		return &call{cf, []sql.CExpr{a1}}, ct, nil
	case *Binary:
		cf := opFuncs[e.Op]
		a1, ct1, err := compile(ctx, pctx, tx, cctx, e.Left, agg)
		if err != nil {
			return nil, ct, err
		}
		a2, ct2, err := compile(ctx, pctx, tx, cctx, e.Right, agg)
		if err != nil {
			return nil, ct, err
		}
		if cf.tfn != nil {
			ct = cf.tfn([]sql.ColumnType{ct1, ct2})
		} else {
			ct = cf.typ
		}
		return &call{cf, []sql.CExpr{a1, a2}}, ct, nil
	case Ref:
		if cctx == nil {
			return nil, ct, fmt.Errorf("engine: %s not found", e)
		}
		idx, nest, ct, err := cctx.CompileRef(e)
		if err != nil {
			return nil, ct, err
		}
		return &colRef{idx: idx, nest: nest, ref: e}, ct, nil
	case *Call:
		cf, ok := idFuncs[e.Name]
		if !ok {
			return nil, ct, fmt.Errorf("engine: function \"%s\" not found", e.Name)
		}
		if len(e.Args) < int(cf.minArgs) {
			return nil, ct,
				fmt.Errorf("engine: function \"%s\": minimum %d arguments got %d", e.Name,
					cf.minArgs, len(e.Args))
		}
		if len(e.Args) > int(cf.maxArgs) {
			return nil, ct,
				fmt.Errorf("engine: function \"%s\": maximum %d arguments got %d", e.Name,
					cf.maxArgs, len(e.Args))
		}

		var actx sql.CompileContext
		if cf.makeAggregator != nil {
			if !agg {
				return nil, ct, &ContextError{e.Name}
			}
			actx = cctx.(aggregatorContext).ArgContext()
		} else {
			actx = cctx
		}

		args := make([]sql.CExpr, len(e.Args))
		argTypes := make([]sql.ColumnType, len(e.Args))
		for i, a := range e.Args {
			var err error
			args[i], argTypes[i], err = compile(ctx, pctx, tx, actx, a, false)
			if err != nil {
				return nil, ct, err
			}
		}
		if cf.tfn != nil {
			ct = cf.tfn(argTypes)
		} else {
			ct = cf.typ
		}

		if cf.makeAggregator == nil {
			return &call{cf, args}, ct, nil
		} else {
			idx := cctx.(aggregatorContext).CompileAggregator(cf.makeAggregator, args)
			return &colRef{idx: idx, ref: Ref{e.Name}}, ct, nil
		}
	case Subquery:
		if pctx == nil {
			return nil, ct, fmt.Errorf("engine: expression statements not allowed here: %s", e.Stmt)
		}

		plan, err := e.Stmt.Plan(ctx, pctx, tx, cctx)
		if err != nil {
			return nil, ct, err
		}

		rowsPlan, ok := plan.(evaluate.RowsPlan)
		if !ok {
			return nil, ct, fmt.Errorf("engine: expected rows: %s", e.Stmt)
		}

		var cf *callFunc
		var ce sql.CExpr
		switch e.Op {
		case Scalar:
			colTypes := rowsPlan.ColumnTypes()
			if len(colTypes) > 0 {
				ct = colTypes[0]
			}
		case Exists:
			ct = sql.BoolColType
		case Any, All:
			ct = sql.BoolColType
			cf = opFuncs[e.ExprOp]
			ce, _, err = compile(ctx, pctx, tx, cctx, e.Expr, agg)
			if err != nil {
				return nil, ct, err
			}
		default:
			panic(fmt.Sprintf("unexpected query expression op; got %v", e.Op))
		}

		return subqueryExpr{
			op:       e.Op,
			call:     cf,
			expr:     ce,
			rowsPlan: rowsPlan,
		}, ct, nil
	case Param:
		if pctx == nil {
			return nil, ct, errors.New("engine: unexpected parameter, not preparing a statement")
		}
		ptr, err := pctx.PlanParameter(e.Num)
		if err != nil {
			return nil, ct, err
		}
		return param{num: e.Num, ptr: ptr}, sql.ColumnType{Type: sql.UnknownType}, nil
	default:
		panic(fmt.Sprintf("missing case for expr: %#v", e))
	}
}

type callFunc struct {
	fn             func(ectx sql.EvalContext, args []sql.Value) (sql.Value, error)
	tfn            func(args []sql.ColumnType) sql.ColumnType
	typ            sql.ColumnType
	minArgs        int16
	maxArgs        int16
	name           string
	handleNull     bool
	makeAggregator MakeAggregator
}

var (
	intType    = sql.ColumnType{Type: sql.IntegerType, Size: 8}
	boolType   = sql.ColumnType{Type: sql.BooleanType}
	stringType = sql.ColumnType{Type: sql.StringType}

	opFuncs = map[Op]*callFunc{
		AddOp:       {fn: addCall, tfn: numType, minArgs: 2, maxArgs: 2},
		AndOp:       {fn: andCall, typ: boolType, minArgs: 2, maxArgs: 2},
		BinaryAndOp: {fn: binaryAndCall, typ: intType, minArgs: 2, maxArgs: 2},
		BinaryOrOp:  {fn: binaryOrCall, typ: intType, minArgs: 2, maxArgs: 2},
		ConcatOp: {fn: concatCall, typ: stringType, minArgs: 2, maxArgs: 2,
			handleNull: true},
		DivideOp:       {fn: divideCall, tfn: numType, minArgs: 2, maxArgs: 2},
		EqualOp:        {fn: equalCall, typ: boolType, minArgs: 2, maxArgs: 2},
		GreaterEqualOp: {fn: greaterEqualCall, typ: boolType, minArgs: 2, maxArgs: 2},
		GreaterThanOp:  {fn: greaterThanCall, typ: boolType, minArgs: 2, maxArgs: 2},
		LessEqualOp:    {fn: lessEqualCall, typ: boolType, minArgs: 2, maxArgs: 2},
		LessThanOp:     {fn: lessThanCall, typ: boolType, minArgs: 2, maxArgs: 2},
		LShiftOp:       {fn: lShiftCall, typ: intType, minArgs: 2, maxArgs: 2},
		ModuloOp:       {fn: moduloCall, typ: intType, minArgs: 2, maxArgs: 2},
		MultiplyOp:     {fn: multiplyCall, tfn: numType, minArgs: 2, maxArgs: 2},
		NegateOp:       {fn: negateCall, tfn: numType, minArgs: 1, maxArgs: 1},
		NotEqualOp:     {fn: notEqualCall, typ: boolType, minArgs: 2, maxArgs: 2},
		NotOp:          {fn: notCall, typ: boolType, minArgs: 1, maxArgs: 1},
		OrOp:           {fn: orCall, typ: boolType, minArgs: 2, maxArgs: 2},
		RShiftOp:       {fn: rShiftCall, typ: intType, minArgs: 2, maxArgs: 2},
		SubtractOp:     {fn: subtractCall, tfn: numType, minArgs: 2, maxArgs: 2},
	}

	idFuncs = map[sql.Identifier]*callFunc{
		// Scalar functions
		sql.ID("abs"): {fn: absCall, tfn: numType, minArgs: 1, maxArgs: 1},
		sql.ID("concat"): {fn: concatCall, typ: stringType, minArgs: 2, maxArgs: math.MaxInt16,
			handleNull: true},
		sql.ID("is_null"): {fn: isNull, typ: boolType, minArgs: 1, maxArgs: 1,
			handleNull: true},
		sql.ID("unique_rowid"): {fn: uniqueRowIDCall, typ: intType, minArgs: 0, maxArgs: 0},
		sql.ID("version"):      {fn: versionCall, typ: stringType, minArgs: 0, maxArgs: 0},

		// Aggregate functions
		sql.ID("avg"): {tfn: numType, minArgs: 1, maxArgs: 1,
			makeAggregator: makeAvgAggregator},
		sql.ID("count"): {typ: intType, minArgs: 1, maxArgs: 1,
			makeAggregator: makeCountAggregator},
		sql.ID("count_all"): {typ: intType,
			minArgs: 0, maxArgs: 0, makeAggregator: makeCountAllAggregator},
		sql.ID("max"): {tfn: numType, minArgs: 1, maxArgs: 1, makeAggregator: makeMaxAggregator},
		sql.ID("min"): {tfn: numType, minArgs: 1, maxArgs: 1, makeAggregator: makeMinAggregator},
		sql.ID("sum"): {tfn: numType, minArgs: 1, maxArgs: 1, makeAggregator: makeSumAggregator},
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
