package query

import (
	"fmt"
	"io"
	"sort"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/sql"
)

type SelectResult interface {
	fmt.Stringer
}

type TableResult struct {
	Table sql.Identifier
}

type ExprResult struct {
	Expr  expr.Expr
	Alias sql.Identifier
}

type OrderBy struct {
	Expr    expr.Expr
	Reverse bool
}

type Select struct {
	Results []SelectResult
	From    FromItem
	Where   expr.Expr
	GroupBy []expr.Expr
	Having  expr.Expr
	OrderBy []OrderBy
}

func (tr TableResult) String() string {
	return fmt.Sprintf("%s.*", tr.Table)
}

func (er ExprResult) String() string {
	s := er.Expr.String()
	if er.Alias != 0 {
		s += fmt.Sprintf(" AS %s", er.Alias)
	}
	return s
}

func (er ExprResult) Column(idx int) sql.Identifier {
	col := er.Alias
	if col == 0 {
		if ref, ok := er.Expr.(expr.Ref); ok && (len(ref) == 1 || len(ref) == 2) {
			// [ table '.' ] column
			if len(ref) == 1 {
				col = ref[0]
			} else {
				col = ref[1]
			}
		} else if call, ok := er.Expr.(*expr.Call); ok {
			col = call.Name
		} else {
			col = sql.ID(fmt.Sprintf("expr%d", idx+1))
		}
	}
	return col
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
	if stmt.GroupBy != nil {
		s += " GROUP BY "
		for i, e := range stmt.GroupBy {
			if i > 0 {
				s += ", "
			}
			s += e.String()
		}
		if stmt.Having != nil {
			s += fmt.Sprintf(" HAVING %s", stmt.Having)
		}
	}
	if stmt.OrderBy != nil {
		s += " ORDER BY "
		for i, by := range stmt.OrderBy {
			if i > 0 {
				s += ", "
			}
			s += by.Expr.String()
			if by.Reverse {
				s += " DESC"
			} else {
				s += " ASC"
			}
		}
	}
	return s
}

func (stmt *Select) Plan(ses evaluate.Session, tx *engine.Transaction) (interface{}, error) {
	return stmt.Rows(ses, tx)
}

func (stmt *Select) Rows(ses evaluate.Session, tx *engine.Transaction) (evaluate.Rows, error) {
	var rows evaluate.Rows
	var fctx *fromContext
	var err error

	if stmt.From == nil {
		rows = &oneEmptyRow{}
		fctx = &fromContext{}
	} else {
		rows, fctx, err = stmt.From.rows(ses, tx)
		if err != nil {
			return nil, err
		}
	}
	rows, err = where(rows, fctx, stmt.Where)
	if err != nil {
		return nil, err
	}
	if stmt.GroupBy == nil && stmt.Having == nil {
		rrows, err := results(rows, fctx, stmt.Results)
		if err == nil {
			return order(rrows, fctx, stmt.OrderBy)
		} else if _, ok := err.(*expr.ContextError); !ok {
			return nil, err
		}
		// Aggregrate function used in SELECT results causes an implicit GROUP BY
	}
	return group(rows, fctx, stmt.Results, stmt.GroupBy, stmt.Having, stmt.OrderBy)
}

func (fs FromSelect) rows(ses evaluate.Session, tx *engine.Transaction) (evaluate.Rows, *fromContext,
	error) {

	rows, err := fs.Select.Rows(ses, tx)
	if err != nil {
		return nil, nil, err
	}
	cols := rows.Columns()
	if fs.ColumnAliases != nil {
		if len(fs.ColumnAliases) != len(cols) {
			return nil, nil, fmt.Errorf("engine: wrong number of column aliases")
		}
		cols = fs.ColumnAliases
	}
	return rows, makeFromContext(fs.Alias, cols), nil
}

type orderBy struct {
	colIndex int
	reverse  bool
}

type sortRows struct {
	rows    evaluate.Rows
	orderBy []orderBy
	values  [][]sql.Value
	index   int
	sorted  bool
}

func (sr *sortRows) Columns() []sql.Identifier {
	return sr.rows.Columns()
}

func (sr *sortRows) Close() error {
	sr.index = len(sr.values)
	return sr.rows.Close()
}

func (sr *sortRows) sort(ses evaluate.Session) error {
	sr.sorted = true

	for {
		dest := make([]sql.Value, len(sr.rows.Columns()))
		err := sr.rows.Next(ses, dest)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		sr.values = append(sr.values, dest)
	}
	sort.Sort(sr)

	return nil
}

func (sr *sortRows) Next(ses evaluate.Session, dest []sql.Value) error {
	if !sr.sorted {
		err := sr.sort(ses)
		if err != nil {
			return err
		}
	}

	if sr.index < len(sr.values) {
		copy(dest, sr.values[sr.index])
		sr.index += 1
		return nil
	}
	return io.EOF
}

func (_ *sortRows) Delete(ses evaluate.Session) error {
	return fmt.Errorf("sort rows may not be deleted")
}

func (_ *sortRows) Update(ses evaluate.Session, updates []db.ColumnUpdate) error {
	return fmt.Errorf("sort rows may not be updated")
}

func (sr *sortRows) Len() int {
	return len(sr.values)
}

func (sr *sortRows) Swap(i, j int) {
	sr.values[i], sr.values[j] = sr.values[j], sr.values[i]
}

func (sr *sortRows) Less(i, j int) bool {
	for _, by := range sr.orderBy {
		vi := sr.values[i][by.colIndex]
		vj := sr.values[j][by.colIndex]
		cmp := sql.Compare(vi, vj)
		if cmp < 0 {
			return !by.reverse
		} else if cmp > 0 {
			return by.reverse
		}
	}
	return false // Arbitrary
}

func orderByOutput(order []OrderBy, cols []sql.Identifier) []orderBy {
	var byOutput []orderBy
	for odx, by := range order {
		r, ok := by.Expr.(expr.Ref)
		if !ok || len(r) != 1 {
			return nil
		}
		for cdx, c := range cols {
			if c == r[0] {
				byOutput = append(byOutput, orderBy{cdx, by.Reverse})
				break
			}
		}
		if len(byOutput) <= odx {
			return nil
		}
	}
	return byOutput
}

func orderByInput(order []OrderBy, fctx *fromContext) []orderBy {
	var byInput []orderBy
	for _, by := range order {
		r, ok := by.Expr.(expr.Ref)
		if !ok || len(r) != 1 {
			return nil
		}
		cdx, err := fctx.colIndex(r[0], "")
		if err != nil {
			return nil
		}
		byInput = append(byInput, orderBy{cdx, by.Reverse})
	}
	return byInput
}

func order(rows evaluate.Rows, fctx *fromContext, order []OrderBy) (evaluate.Rows, error) {
	if order == nil {
		return rows, nil
	}

	// ORDER BY is based on output columns
	byCols := orderByOutput(order, rows.Columns())
	if byCols != nil {
		return &sortRows{rows: rows, orderBy: byCols}, nil
	}

	// ORDER BY is based on input columns
	byCols = orderByInput(order, fctx)
	if byCols != nil {
		if rrows, ok := rows.(*resultRows); ok {
			rrows.rows = &sortRows{rows: rrows.rows, orderBy: byCols}
			return rrows, nil
		} else if arows, ok := rows.(*allResultRows); ok {
			arows.rows = &sortRows{rows: arows.rows, orderBy: byCols}
			return arows, nil
		} else {
			panic("must be resultRows or allResultRows")
		}
	}

	// ORDER BY is based on arbitrary input column expressions
	return rows, fmt.Errorf("ORDER BY arbitrary input column expressions is not supported")
}

type filterRows struct {
	rows evaluate.Rows
	cond expr.CExpr
	dest []sql.Value
}

func (fr *filterRows) EvalRef(idx int) sql.Value {
	return fr.dest[idx]
}

func (fr *filterRows) Columns() []sql.Identifier {
	return fr.rows.Columns()
}

func (fr *filterRows) Close() error {
	return fr.rows.Close()
}

func (fr *filterRows) Next(ses evaluate.Session, dest []sql.Value) error {
	for {
		err := fr.rows.Next(ses, dest)
		if err != nil {
			return err
		}
		fr.dest = dest
		defer func() {
			fr.dest = nil
		}()
		v, err := fr.cond.Eval(fr)
		if err != nil {
			return err
		}
		b, ok := v.(sql.BoolValue)
		if !ok {
			return fmt.Errorf("engine: expected boolean result from WHERE condition: %s",
				sql.Format(v))
		}
		if b {
			break
		}
	}
	return nil
}

func (fr *filterRows) Delete(ses evaluate.Session) error {
	return fr.rows.Delete(ses)
}

func (fr *filterRows) Update(ses evaluate.Session, updates []db.ColumnUpdate) error {
	return fr.rows.Update(ses, updates)
}

func where(rows evaluate.Rows, fctx *fromContext, cond expr.Expr) (evaluate.Rows, error) {
	if cond == nil {
		return rows, nil
	}
	ce, err := expr.Compile(fctx, cond, false)
	if err != nil {
		return nil, err
	}
	return &filterRows{rows: rows, cond: ce}, nil
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

func (oer *oneEmptyRow) Next(ses evaluate.Session, dest []sql.Value) error {
	if oer.one {
		return io.EOF
	}
	oer.one = true
	return nil
}

func (_ *oneEmptyRow) Delete(ses evaluate.Session) error {
	return fmt.Errorf("one empty row may not be deleted")
}

func (_ *oneEmptyRow) Update(ses evaluate.Session, updates []db.ColumnUpdate) error {
	return fmt.Errorf("one empty row may not be updated")
}

type allResultRows struct {
	rows    evaluate.Rows
	columns []sql.Identifier
}

func (arr *allResultRows) Columns() []sql.Identifier {
	return arr.columns
}

func (arr *allResultRows) Close() error {
	return arr.rows.Close()
}

func (arr *allResultRows) Next(ses evaluate.Session, dest []sql.Value) error {
	return arr.rows.Next(ses, dest)
}

func (_ *allResultRows) Delete(ses evaluate.Session) error {
	return fmt.Errorf("all result rows may not be deleted")
}

func (_ *allResultRows) Update(ses evaluate.Session, updates []db.ColumnUpdate) error {
	return fmt.Errorf("all result rows may not be updated")
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
	rows      evaluate.Rows
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

func (rr *resultRows) Next(ses evaluate.Session, dest []sql.Value) error {
	if rr.dest == nil {
		rr.dest = make([]sql.Value, len(rr.rows.Columns()))
	}
	err := rr.rows.Next(ses, rr.dest)
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

func (_ *resultRows) Delete(ses evaluate.Session) error {
	return fmt.Errorf("result rows may not be deleted")
}

func (_ *resultRows) Update(ses evaluate.Session, updates []db.ColumnUpdate) error {
	return fmt.Errorf("result rows may not be updated")
}

func results(rows evaluate.Rows, fctx *fromContext, results []SelectResult) (evaluate.Rows, error) {
	if results == nil {
		return &allResultRows{rows: rows, columns: fctx.columns()}, nil
	}

	var destExprs []expr2dest
	var cols []sql.Identifier
	ddx := 0
	for _, sr := range results {
		switch sr := sr.(type) {
		case TableResult:
			for _, col := range fctx.tableColumns(sr.Table) {
				ce, err := expr.Compile(fctx, expr.Ref{sr.Table, col}, false)
				if err != nil {
					panic(err)
				}
				destExprs = append(destExprs, expr2dest{destColIndex: ddx, expr: ce})
				cols = append(cols, col)
				ddx += 1
			}
		case ExprResult:
			ce, err := expr.Compile(fctx, sr.Expr, false)
			if err != nil {
				return nil, err
			}
			destExprs = append(destExprs, expr2dest{destColIndex: ddx, expr: ce})
			cols = append(cols, sr.Column(len(cols)))
			ddx += 1
		default:
			panic(fmt.Sprintf("unexpected type for query.SelectResult: %T: %v", sr, sr))
		}
	}
	return makeResultRows(rows, cols, destExprs), nil
}

func makeResultRows(rows evaluate.Rows, cols []sql.Identifier,
	destExprs []expr2dest) evaluate.Rows {

	rr := resultRows{rows: rows, columns: cols}
	for _, de := range destExprs {
		if ci, ok := expr.ColumnIndex(de.expr); ok {
			rr.destCols = append(rr.destCols,
				src2dest{destColIndex: de.destColIndex, srcColIndex: ci})
		} else {
			rr.destExprs = append(rr.destExprs, de)
		}
	}
	if rr.destExprs != nil || len(rows.Columns()) != len(cols) {
		return &rr
	}
	for cdx, dc := range rr.destCols {
		if dc.destColIndex != cdx || dc.srcColIndex != cdx {
			return &rr
		}
	}
	return &allResultRows{rows: rows, columns: cols}
}