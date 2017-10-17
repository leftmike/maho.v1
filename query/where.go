package query

import (
	"maho/engine"
	"maho/expr"
)

func where(e *engine.Engine, rows *fromRows, cond expr.Expr) (*fromRows, error) {
	/*
		ctx, _ := rows.(expr.CompileContext) // XXX
		_, err := expr.Compile(ctx, stmt.Where)
		if err != nil {
			return nil, err
		}
	*/
	return rows, nil
}
