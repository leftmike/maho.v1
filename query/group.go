package query

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/sql"
)

type groupRows struct {
	rows       db.Rows
	dest       []sql.Value
	columns    []sql.Identifier
	groupExprs []expr2dest
	groups     [][]sql.Value
	index      int
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

func (gr *groupRows) Next(dest []sql.Value) error {
	if gr.dest == nil {
		gr.dest = make([]sql.Value, len(gr.rows.Columns()))
		groups := map[string][]sql.Value{}
		for {
			err := gr.rows.Next(gr.dest)
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			row := make([]sql.Value, len(gr.columns))
			var key string
			for _, e2d := range gr.groupExprs {
				val, err := e2d.expr.Eval(gr)
				if err != nil {
					return err
				}
				row[e2d.destColIndex] = val
				key = fmt.Sprintf("%s[%s]", key, val)
			}
			if _, ok := groups[key]; !ok {
				groups[key] = row
			} // else aggregrate
		}
		gr.rows.Close()

		gr.groups = make([][]sql.Value, 0, len(groups))
		for _, row := range groups {
			gr.groups = append(gr.groups, row)
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
	fctx *fromContext
}

func (gc *groupContext) CompileRef(r expr.Ref) (int, error) {
	return 0, fmt.Errorf("engine: column \"%s\" must appear in a GROUP BY clause or in an "+
		"aggregrate function", r)
}

func group(rows db.Rows, fctx *fromContext, resultsXXX []SelectResult, group []expr.Expr,
	having expr.Expr) (db.Rows, error) {

	if group == nil {
		panic("GroupBy == nil is not implemented")
	}

	var groupExprs []expr2dest
	var groupCols []sql.Identifier
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
		ddx += 1
	}

	var destExprs []expr2dest
	var resultCols []sql.Identifier
	gctx := &groupContext{fctx: fctx}
	for ddx, sr := range resultsXXX {
		er, ok := sr.(ExprResult)
		if !ok {
			panic(fmt.Sprintf("unexpected type for query.SelectResult: %T: %v", sr, sr))
		}
		for gdx, e := range group {
			if er.Expr.Equal(e) {
				destExprs = append(destExprs,
					expr2dest{destColIndex: ddx, expr: expr.CompileRef(gdx)})
				resultCols = append(resultCols, groupCols[gdx])
			} else {
				ce, err := expr.Compile(gctx, er.Expr, true)
				if err != nil {
					return nil, err
				}
				destExprs = append(destExprs, expr2dest{destColIndex: ddx, expr: ce})
				resultCols = append(resultCols, er.Column(len(resultCols)))
			}
		}
	}

	// return &groupRows{rows: rows, columns: groupCols, groupExprs: groupExprs}, nil
	rows = &groupRows{rows: rows, columns: groupCols, groupExprs: groupExprs}
	// return results(rows, makeFromContext(0, rows.Columns()), resultsXXX)
	return makeResultRows(rows, resultCols, destExprs), nil
	// change resultsXXX to results
}

//return nil, fmt.Errorf("GROUP BY and HAVING not implemented yet")
/*
	for _, e := range stmt.GroupBy {
		ce, err := expr.Compile(fctx, e)
		colExprs = append(colExprs, expr2dest{destCol, ce})
		cols = append(cols, col) // actually need the expr as well
	}
	groupCount = destCol

	rows, err = results(rows, fctx, stmt.GroupBy)
	if err != nil {
		return nil, err
	}
	// gctx is based on stmt.GroupBy
	// determine list of

	var gctx *groupContext
	if stmt.GroupBy {
		rows, gctx, err = group(rows, fctx, stmt.GroupBy)
		if err != nil {
			return nil, err
		}
	} else {
		gctx = fctx
		rows = oneGroup(rows)
	}
	if stmt.Having {
		rows, err = having(rows, gctx, stmt.Having)
		if err != nil {
			return nil, err
		}
	}
	return results(rows, gctx, stmt.Results)
*/
