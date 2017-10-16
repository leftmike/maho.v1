package query

import (
	"maho/engine"
	"maho/expr"
)

func Where(e *engine.Engine, rows Rows, cond expr.Expr) (Rows, error) {
	/*
		ctx, _ := rows.(expr.CompileContext) // XXX
		_, err := expr.Compile(ctx, stmt.Where)
		if err != nil {
			return nil, err
		}
	*/
	return rows, nil
}
