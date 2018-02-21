package query

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/sql"
)

type aggregator struct {
	maker expr.MakeAggregator
	args  []expr.CExpr
}

type groupRows struct {
	rows        db.Rows
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
	return gr.rows.Close()
}

type groupRow struct {
	row         []sql.Value
	aggregators []expr.Aggregator
}

func (gr *groupRows) group() error {
	gr.dest = make([]sql.Value, len(gr.rows.Columns()))
	groups := map[string]groupRow{}
	for {
		err := gr.rows.Next(gr.dest)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		row := make([]sql.Value, len(gr.groupExprs)+len(gr.aggregators))
		var key string
		for _, e2d := range gr.groupExprs {
			val, err := e2d.expr.Eval(gr)
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
				val, err := gr.aggregators[adx].args[idx].Eval(gr)
				if err != nil {
					return err
				}
				args[idx] = val
			}
			group.aggregators[adx].Accumulate(args)
		}
	}
	gr.rows.Close()

	gr.groups = make([][]sql.Value, 0, len(groups))
	for _, group := range groups {
		cdx := len(gr.groupExprs)
		for adx := range group.aggregators {
			group.row[cdx] = group.aggregators[adx].Total()
			cdx += 1
		}
		gr.groups = append(gr.groups, group.row)
	}
	return nil
}

func (gr *groupRows) Next(dest []sql.Value) error {
	if gr.dest == nil {
		err := gr.group()
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

type groupContext struct {
	group       []expr.Expr
	groupRefs   []bool
	aggregators []*expr.Call
	makers      []expr.MakeAggregator
}

func (gc *groupContext) CompileRef(r expr.Ref) (int, error) {
	return 0, fmt.Errorf("engine: column \"%s\" must appear in a GROUP BY clause or in an "+
		"aggregate function", r)
}

func (gc *groupContext) MaybeRefExpr(e expr.Expr) (int, bool) {
	for gdx, ge := range gc.group {
		if gc.groupRefs[gdx] && e.Equal(ge) {
			return gdx, true
		}
	}
	return 0, false
}

func (gc *groupContext) CompileAggregator(c *expr.Call, maker expr.MakeAggregator) int {
	for adx, ae := range gc.aggregators {
		if ae.Equal(c) {
			return adx + len(gc.group)
		}
	}
	gc.aggregators = append(gc.aggregators, c)
	gc.makers = append(gc.makers, maker)
	return len(gc.group) + len(gc.aggregators) - 1
}

func group(rows db.Rows, fctx *fromContext, results []SelectResult, group []expr.Expr,
	having expr.Expr) (db.Rows, error) {

	if group == nil {
		panic("GroupBy == nil is not implemented")
	}

	var groupExprs []expr2dest
	var groupCols []sql.Identifier
	var groupRefs []bool
	ddx := 0
	for _, e := range group {
		ce, err := expr.Compile(fctx, e, false)
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

	var destExprs []expr2dest
	var resultCols []sql.Identifier
	gctx := &groupContext{group: group, groupRefs: groupRefs}
	for ddx, sr := range results {
		er, ok := sr.(ExprResult)
		if !ok {
			panic(fmt.Sprintf("unexpected type for query.SelectResult: %T: %v", sr, sr))
		}
		ce, err := expr.Compile(gctx, er.Expr, true)
		if err != nil {
			return nil, err
		}
		destExprs = append(destExprs, expr2dest{destColIndex: ddx, expr: ce})
		resultCols = append(resultCols, er.Column(len(resultCols)))
	}

	grows := &groupRows{rows: rows, columns: groupCols, groupExprs: groupExprs}
	for idx := range gctx.aggregators {
		agg := aggregator{maker: gctx.makers[idx]}
		for _, a := range gctx.aggregators[idx].Args {
			ce, err := expr.Compile(fctx, a, false)
			if err != nil {
				return nil, err
			}
			agg.args = append(agg.args, ce)
		}
		grows.aggregators = append(grows.aggregators, agg)
		grows.columns = append(grows.columns, sql.ID(fmt.Sprintf("agg%d", len(grows.columns)+1)))
	}

	return makeResultRows(grows, resultCols, destExprs), nil
}
