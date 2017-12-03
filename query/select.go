package query

import (
	"fmt"
	"io"

	"maho/db"
	"maho/engine"
	"maho/expr"
	"maho/sql"
)

type SelectResult interface {
	fmt.Stringer
}

type TableResult struct {
	Table sql.Identifier
}

type TableColumnResult struct {
	Table  sql.Identifier
	Column sql.Identifier
	Alias  sql.Identifier
}

type ExprResult struct {
	Expr  expr.Expr
	Alias sql.Identifier
}

type Select struct {
	Results []SelectResult
	From    FromItem
	Where   expr.Expr
}

func (tr TableResult) String() string {
	return fmt.Sprintf("%s.*", tr.Table)
}

func (tcr TableColumnResult) String() string {
	var s string
	if tcr.Table == 0 {
		s = tcr.Column.String()
	} else {
		s = fmt.Sprintf("%s.%s", tcr.Table, tcr.Column)
	}
	if tcr.Alias != 0 {
		s += fmt.Sprintf(" AS %s", tcr.Alias)
	}
	return s
}

func (er ExprResult) String() string {
	s := er.Expr.String()
	if er.Alias != 0 {
		s += fmt.Sprintf(" AS %s", er.Alias)
	}
	return s
}

type FromSelect struct {
	Select
	Alias         sql.Identifier
	ColumnAliases []sql.Identifier
}

func (fs FromSelect) String() string {
	s := fmt.Sprintf("(%s) AS %s", fs.Select.String(), fs.Alias)
	if fs.ColumnAliases != nil {
		s += " ("
		for i, col := range fs.ColumnAliases {
			if i > 0 {
				s += ", "
			}
			s += col.String()
		}
		s += ")"
	}
	return s
}

func (stmt *Select) String() string {
	s := "SELECT "
	if stmt.Results == nil {
		s += "*"
	} else {
		for i, sr := range stmt.Results {
			if i > 0 {
				s += ", "
			}
			s += sr.String()
		}
	}
	s += fmt.Sprintf(" FROM %s", stmt.From)
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

func (stmt *Select) Rows(e *engine.Engine) (db.Rows, error) {
	var rows db.Rows
	var fctx *fromContext
	var err error

	if stmt.From == nil {
		rows = &oneEmptyRow{}
	} else {
		rows, fctx, err = stmt.From.rows(e)
		if err != nil {
			return nil, err
		}
	}
	rows, err = where(rows, fctx, stmt.Where)
	if err != nil {
		return nil, err
	}
	return results(rows, fctx, stmt.Results)
}

func (fs FromSelect) rows(e *engine.Engine) (db.Rows, *fromContext, error) {
	rows, err := fs.Select.Rows(e)
	if err != nil {
		return nil, nil, err
	}
	cols := rows.Columns()
	if fs.ColumnAliases != nil {
		if len(fs.ColumnAliases) != len(cols) {
			return nil, nil, fmt.Errorf("wrong number of column aliases")
		}
		cols = fs.ColumnAliases
	}
	return rows, makeFromContext(fs.Alias, cols), nil
}

type whereRows struct {
	rows db.Rows
	cond expr.CExpr
	dest []sql.Value
}

func (wr *whereRows) EvalRef(idx int) sql.Value {
	return wr.dest[idx]
}

func (wr *whereRows) Columns() []sql.Identifier {
	return wr.rows.Columns()
}

func (wr *whereRows) Close() error {
	return wr.rows.Close()
}

func (wr *whereRows) Next(dest []sql.Value) error {
	for {
		err := wr.rows.Next(dest)
		if err != nil {
			return err
		}
		wr.dest = dest
		defer func() {
			wr.dest = nil
		}()
		v, err := wr.cond.Eval(wr)
		if err != nil {
			return err
		}
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("expected boolean result from WHERE condition: %s", sql.Format(v))
		}
		if b {
			break
		}
	}
	return nil
}

func where(rows db.Rows, fctx *fromContext, cond expr.Expr) (db.Rows, error) {
	if cond == nil {
		return rows, nil
	}
	ce, err := expr.Compile(fctx, cond)
	if err != nil {
		return nil, err
	}
	return &whereRows{rows: rows, cond: ce}, nil
}

type oneEmptyRow struct {
	one bool
}

func (oer *oneEmptyRow) Columns() []sql.Identifier {
	return []sql.Identifier{}
}

func (oer *oneEmptyRow) Close() error {
	oer.one = true
	return nil
}

func (oer *oneEmptyRow) Next(dest []sql.Value) error {
	if oer.one {
		return io.EOF
	}
	oer.one = true
	return nil
}

type allResultRows struct {
	rows    db.Rows
	columns []sql.Identifier
}

func (arr *allResultRows) Columns() []sql.Identifier {
	return arr.columns
}

func (arr *allResultRows) Close() error {
	return arr.rows.Close()
}

func (arr *allResultRows) Next(dest []sql.Value) error {
	return arr.rows.Next(dest)
}

type src2dest struct {
	destColIndex int
	srcColIndex  int
}

type expr2dest struct {
	destColIndex int
	expr         expr.CExpr
}

type resultRows struct {
	rows      db.Rows
	dest      []sql.Value
	columns   []sql.Identifier
	destCols  []src2dest
	destExprs []expr2dest
}

func (rr *resultRows) EvalRef(idx int) sql.Value {
	return rr.dest[idx]
}

func (rr *resultRows) Columns() []sql.Identifier {
	return rr.columns
}

func (rr *resultRows) Close() error {
	return rr.rows.Close()
}

func (rr *resultRows) Next(dest []sql.Value) error {
	if rr.dest == nil {
		rr.dest = make([]sql.Value, len(rr.rows.Columns()))
	}
	err := rr.rows.Next(rr.dest)
	if err != nil {
		return err
	}
	for _, c2d := range rr.destCols {
		dest[c2d.destColIndex] = rr.dest[c2d.srcColIndex]
	}
	for _, e2d := range rr.destExprs {
		val, err := e2d.expr.Eval(rr)
		if err != nil {
			return err
		}
		dest[e2d.destColIndex] = val
	}
	return nil
}

func results(rows db.Rows, fctx *fromContext, results []SelectResult) (db.Rows, error) {
	if results == nil {
		return &allResultRows{rows: rows, columns: fctx.columns()}, nil
	}

	var destCols []src2dest
	var destExprs []expr2dest
	var cols []sql.Identifier
	cdx := 0
	edx := 0
	for _, sr := range results {
		switch sr := sr.(type) {
		case TableResult:
			tblCols, tblIdxs := fctx.tableColumns(sr.Table)
			for idx, col := range tblCols {
				destCols = append(destCols, src2dest{destColIndex: cdx, srcColIndex: tblIdxs[idx]})
				cols = append(cols, col)
				cdx += 1
			}
		case TableColumnResult:
			rdx, err := fctx.tblColIndex(sr.Table, sr.Column, "result")
			if err != nil {
				return nil, err
			}
			destCols = append(destCols, src2dest{destColIndex: cdx, srcColIndex: rdx})
			col := sr.Column
			if sr.Alias != 0 {
				col = sr.Alias
			}
			cols = append(cols, col)
			cdx += 1
		case ExprResult:
			ce, err := expr.Compile(fctx, sr.Expr)
			if err != nil {
				return nil, err
			}
			destExprs = append(destExprs, expr2dest{destColIndex: edx, expr: ce})
			col := sr.Alias
			if col == 0 {
				col = sql.ID(fmt.Sprintf("expr%d", len(cols)+1))
			}
			cols = append(cols, col)
			edx += 1
		default:
			panic(fmt.Sprintf("unexpected type for query.SelectResult: %T: %v", sr, sr))
		}
	}
	return &resultRows{rows: rows, columns: cols, destCols: destCols, destExprs: destExprs}, nil
}
