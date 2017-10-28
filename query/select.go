package query

import (
	"fmt"
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
	Alias sql.Identifier
}

func (fs FromSelect) String() string {
	s := fmt.Sprintf("(%s)", fs.Select.String())
	if fs.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fs.Alias)
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
	if stmt.From == nil {
		return nil, fmt.Errorf("SELECT with no FROM clause is not supported yet")
	}
	rows, fctx, err := stmt.From.rows(e)
	if err != nil {
		return nil, err
	}
	rows, err = where(rows, fctx, stmt.Where)
	if err != nil {
		return nil, err
	}
	rows, err = results(rows, fctx, stmt.Results)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (fs FromSelect) rows(e *engine.Engine) (db.Rows, *fromContext, error) {
	rows, err := fs.Select.Rows(e)
	if err != nil {
		return nil, nil, err
	}
	return rows, makeFromContext(fs.Alias, rows.Columns()), nil
}

type whereRows struct {
	rows db.Rows
	cond expr.CExpr
	dest []sql.Value
}

func (wr *whereRows) EvalRef(idx int) (sql.Value, error) {
	return wr.dest[idx], nil
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
		v, err := wr.cond.Eval(wr)
		wr.dest = nil
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

type col2dest struct {
	destIndex int
	rowIndex  int
}

type resultRows struct {
	rows     db.Rows
	rowDest  []sql.Value
	columns  []sql.Identifier
	row2dest []col2dest
}

func (rr *resultRows) Columns() []sql.Identifier {
	return rr.columns
}

func (rr *resultRows) Close() error {
	return rr.rows.Close()
}

func (rr *resultRows) Next(dest []sql.Value) error {
	if rr.rowDest == nil {
		rr.rowDest = make([]sql.Value, len(rr.rows.Columns()))
	}
	err := rr.rows.Next(rr.rowDest)
	if err != nil {
		return err
	}
	for _, r2d := range rr.row2dest {
		dest[r2d.destIndex] = rr.rowDest[r2d.rowIndex]
	}
	return nil
}

func results(rows db.Rows, fctx *fromContext, results []SelectResult) (db.Rows, error) {
	if results == nil {
		return rows, nil
	}

	var row2dest []col2dest
	var cols []sql.Identifier
	idx := 0
	for _, sr := range results {
		switch sr := sr.(type) {
		case TableResult:
			for _, ci := range fctx.tableColumns(sr.Table) {
				row2dest = append(row2dest, col2dest{destIndex: idx, rowIndex: ci.index})
				cols = append(cols, ci.column)
				idx += 1
			}
		case TableColumnResult:
			rdx, err := fctx.columnIndex(sr.Table, sr.Column, "result")
			if err != nil {
				return nil, err
			}
			row2dest = append(row2dest, col2dest{destIndex: idx, rowIndex: rdx})
			col := sr.Column
			if sr.Alias != 0 {
				col = sr.Alias
			}
			cols = append(cols, col)
			idx += 1
		case ExprResult:
			// XXX
			// type expr2dest struct { destIndex int, expr expr.CExpr }
			// []expr2dest
			return nil, fmt.Errorf("ExprResult not implemented")
		default:
			panic(fmt.Sprintf("unexpected type for query.SelectResult: %T: %v", sr, sr))
		}
	}
	return &resultRows{rows: rows, columns: cols, row2dest: row2dest}, nil
}
