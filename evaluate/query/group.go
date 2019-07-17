package query

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

type aggregator struct {
	maker expr.MakeAggregator
	args  []expr.CExpr
}

type groupRows struct {
	rows        engine.Rows
	dest        []sql.Value
	columns     []sql.Identifier
	groupExprs  []expr2dest
	aggregators []aggregator
	groups      [][]sql.Value
	index       int
}

func (gr *groupRows) EvalRef(idx int) sql.Value {
	return gr.dest[idx]
}

func (gr *groupRows) Columns() []sql.Identifier {
	return gr.columns
}

func (gr *groupRows) Close() error {
	gr.index = len(gr.groups)
	return gr.rows.Close()
}

type groupRow struct {
	row         []sql.Value
	aggregators []expr.Aggregator
}

func (gr *groupRows) group(ctx context.Context) error {
	gr.dest = make([]sql.Value, len(gr.rows.Columns()))
	groups := map[string]groupRow{}
	for {
		err := gr.rows.Next(ctx, gr.dest)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		row := make([]sql.Value, len(gr.groupExprs)+len(gr.aggregators))
		var key string
		for _, e2d := range gr.groupExprs {
			val, err := e2d.expr.Eval(ctx, gr)
			if err != nil {
				return err
			}
			row[e2d.destColIndex] = val
			key = fmt.Sprintf("%s[%s]", key, val)
		}
		group, ok := groups[key]
		if !ok {
			group = groupRow{row: row, aggregators: make([]expr.Aggregator, len(gr.aggregators))}
			for adx := range gr.aggregators {
				group.aggregators[adx] = gr.aggregators[adx].maker()
			}
			groups[key] = group
		}
		for adx := range gr.aggregators {
			args := make([]sql.Value, len(gr.aggregators[adx].args))
			for idx := range gr.aggregators[adx].args {
				val, err := gr.aggregators[adx].args[idx].Eval(ctx, gr)
				if err != nil {
					return err
				}
				args[idx] = val
			}
			err := group.aggregators[adx].Accumulate(args)
			if err != nil {
				return err
			}
		}
	}
	gr.rows.Close()

	gr.groups = make([][]sql.Value, 0, len(groups))
	for _, group := range groups {
		cdx := len(gr.groupExprs)
		for adx := range group.aggregators {
			val, err := group.aggregators[adx].Total()
			if err != nil {
				return err
			}
			group.row[cdx] = val
			cdx += 1
		}
		gr.groups = append(gr.groups, group.row)
	}
	return nil
}

func (gr *groupRows) Next(ctx context.Context, dest []sql.Value) error {
	if gr.dest == nil {
		err := gr.group(ctx)
		if err != nil {
			return err
		}
	}

	if gr.index < len(gr.groups) {
		copy(dest, gr.groups[gr.index])
		gr.index += 1
		return nil
	}
	return io.EOF
}

func (_ *groupRows) Delete(ctx context.Context) error {
	return fmt.Errorf("group rows may not be deleted")
}

func (_ *groupRows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	return fmt.Errorf("group rows may not be updated")
}

type groupContext struct {
	group       []sql.Expr
	groupExprs  []expr2dest
	groupCols   []sql.Identifier
	groupRefs   []bool
	aggregators []*expr.Call
	makers      []expr.MakeAggregator
}

func (_ *groupContext) CompileRef(r expr.Ref) (int, error) {
	return 0, fmt.Errorf("engine: column \"%s\" must appear in a GROUP BY clause or in an "+
		"aggregate function", r)
}

func (gctx *groupContext) MaybeRefExpr(e sql.Expr) (int, bool) {
	for gdx, ge := range gctx.group {
		if gctx.groupRefs[gdx] && e.Equal(ge) {
			return gdx, true
		}
	}
	return 0, false
}

func (gctx *groupContext) CompileAggregator(c *expr.Call, maker expr.MakeAggregator) int {
	for adx, ae := range gctx.aggregators {
		if ae.Equal(c) {
			return adx + len(gctx.group)
		}
	}
	gctx.aggregators = append(gctx.aggregators, c)
	gctx.makers = append(gctx.makers, maker)
	return len(gctx.group) + len(gctx.aggregators) - 1
}

func (gctx *groupContext) makeGroupRows(ses *evaluate.Session, tx engine.Transaction,
	rows engine.Rows, fctx *fromContext) (engine.Rows, error) {

	gr := &groupRows{rows: rows, columns: gctx.groupCols, groupExprs: gctx.groupExprs}
	for idx := range gctx.aggregators {
		agg := aggregator{maker: gctx.makers[idx]}
		for _, a := range gctx.aggregators[idx].Args {
			ce, err := expr.Compile(ses, tx, fctx, a, false)
			if err != nil {
				return nil, err
			}
			agg.args = append(agg.args, ce)
		}
		gr.aggregators = append(gr.aggregators, agg)
		gr.columns = append(gr.columns, sql.ID(fmt.Sprintf("agg%d", len(gr.columns)+1)))
	}
	return gr, nil
}

func makeGroupContext(ses *evaluate.Session, tx engine.Transaction, fctx *fromContext,
	group []sql.Expr) (*groupContext, error) {

	var groupExprs []expr2dest
	var groupCols []sql.Identifier
	var groupRefs []bool
	ddx := 0
	for _, e := range group {
		ce, err := expr.Compile(ses, tx, fctx, e, false)
		if err != nil {
			return nil, err
		}
		groupExprs = append(groupExprs, expr2dest{destColIndex: ddx, expr: ce})
		if ref, ok := e.(expr.Ref); ok {
			groupCols = append(groupCols, ref[0])
		} else {
			groupCols = append(groupCols, sql.ID(fmt.Sprintf("expr%d", len(groupCols)+1)))
		}
		groupRefs = append(groupRefs, e.HasRef())
		ddx += 1
	}

	return &groupContext{group: group, groupExprs: groupExprs, groupCols: groupCols,
		groupRefs: groupRefs}, nil

}

func group(ses *evaluate.Session, tx engine.Transaction, rows engine.Rows, fctx *fromContext,
	results []SelectResult, group []sql.Expr, having sql.Expr, orderBy []OrderBy) (engine.Rows,
	error) {

	gctx, err := makeGroupContext(ses, tx, fctx, group)

	var destExprs []expr2dest
	var resultCols []sql.Identifier
	for ddx, sr := range results {
		er, ok := sr.(ExprResult)
		if !ok {
			panic(fmt.Sprintf("unexpected type for query.SelectResult: %T: %v", sr, sr))
		}
		var ce expr.CExpr
		ce, err = expr.Compile(ses, tx, gctx, er.Expr, true)
		if err != nil {
			return nil, err
		}
		destExprs = append(destExprs, expr2dest{destColIndex: ddx, expr: ce})
		resultCols = append(resultCols, er.Column(len(resultCols)))
	}

	var hce expr.CExpr
	if having != nil {
		hce, err = expr.Compile(ses, tx, gctx, having, true)
		if err != nil {
			return nil, err
		}
	}

	rows, err = gctx.makeGroupRows(ses, tx, rows, fctx)
	if err != nil {
		return nil, err
	}

	if having != nil {
		rows = &filterRows{rows: rows, cond: hce}
	}

	rrows := makeResultRows(rows, resultCols, destExprs)
	if orderBy == nil {
		return rrows, nil
	}
	return order(rrows, makeFromContext(0, rows.Columns()), orderBy)
}
