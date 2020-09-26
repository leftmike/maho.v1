package query

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
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

func (stmt *Select) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction) (evaluate.Plan, error) {

	var rop rowsOp
	var fctx *fromContext
	var err error

	if stmt.From == nil {
		rop = oneEmptyOp{}
		fctx = &fromContext{}
	} else {
		rop, fctx, err = stmt.From.plan(ctx, pctx, tx)
		if err != nil {
			return nil, err
		}
	}

	rop, err = where(ctx, pctx, tx, rop, fctx, stmt.Where)
	if err != nil {
		return nil, err
	}

	if stmt.GroupBy == nil && stmt.Having == nil {
		rrop, err := results(ctx, pctx, tx, rop, fctx, stmt.Results)
		if err == nil {
			if stmt.OrderBy == nil {
				return rowsOpPlan{rop: rrop, cols: rrop.columns()}, nil
			}

			rop, err = order(rrop, fctx, stmt.OrderBy)
			if err != nil {
				return nil, err
			}
			return rowsOpPlan{rop: rop, cols: rrop.columns()}, nil
		} else if _, ok := err.(*expr.ContextError); !ok {
			return nil, err
		}
		// Aggregrate function used in SELECT results causes an implicit GROUP BY
	}

	return group(ctx, pctx, tx, rop, fctx, stmt.Results, stmt.GroupBy, stmt.Having, stmt.OrderBy)
}

type rowsOpPlan struct {
	rop  rowsOp
	cols []sql.Identifier
}

func (_ rowsOpPlan) Planned() {}

func (rp rowsOpPlan) Columns() []sql.Identifier {
	return rp.cols
}

func (rp rowsOpPlan) Rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	return rp.rop.rows(ctx, tx)
}

func (rp rowsOpPlan) Explain() evaluate.ExplainTree {
	return rp.rop
}

type sortOp struct {
	rop     rowsOp
	orderBy []orderBy
}

func (_ sortOp) Name() string {
	return "sort"
}

func (so sortOp) Columns() []string {
	return so.rop.Columns()
}

func (so sortOp) Fields() []evaluate.FieldDescription {
	cols := so.rop.Columns()
	var desc string
	for _, ob := range so.orderBy {
		if desc != "" {
			desc += ", "
		}
		if ob.reverse {
			desc += "-"
		} else {
			desc += "+"
		}
		desc += cols[ob.colIndex]
	}

	return []evaluate.FieldDescription{
		{Field: "order", Description: desc},
	}
}

func (so sortOp) Children() []evaluate.ExplainTree {
	return []evaluate.ExplainTree{so.rop}
}

func (so sortOp) rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	r, err := so.rop.rows(ctx, tx)
	if err != nil {
		return nil, err
	}

	return &sortRows{rows: r, orderBy: so.orderBy}, nil
}

type orderBy struct {
	colIndex int
	reverse  bool
}

type sortRows struct {
	rows    sql.Rows
	orderBy []orderBy
	values  [][]sql.Value
	index   int
	sorted  bool
}

func (sr *sortRows) NumColumns() int {
	return sr.rows.NumColumns()
}

func (sr *sortRows) Close() error {
	sr.index = len(sr.values)
	return sr.rows.Close()
}

func (sr *sortRows) sort(ctx context.Context) error {
	sr.sorted = true

	for {
		dest := make([]sql.Value, sr.rows.NumColumns())
		err := sr.rows.Next(ctx, dest)
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

func (sr *sortRows) Next(ctx context.Context, dest []sql.Value) error {
	if !sr.sorted {
		err := sr.sort(ctx)
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

func (_ *sortRows) Delete(ctx context.Context) error {
	return fmt.Errorf("sort rows may not be deleted")
}

func (_ *sortRows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
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
	return false
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

func order(rrop resultRowsOp, fctx *fromContext, order []OrderBy) (rowsOp, error) {
	// ORDER BY is based on output columns
	byCols := orderByOutput(order, rrop.columns())
	if byCols != nil {
		return sortOp{rop: rrop, orderBy: byCols}, nil
	}

	// ORDER BY is based on input columns
	byCols = orderByInput(order, fctx)
	if byCols != nil {
		if aro, ok := rrop.(*allResultsOp); ok {
			aro.rop = sortOp{rop: aro.rop, orderBy: byCols}
			return aro, nil
		} else if ro, ok := rrop.(*resultsOp); ok {
			ro.rop = sortOp{rop: ro.rop, orderBy: byCols}
			return ro, nil
		} else {
			panic("must be allResultsOp or resultsOp")
		}
	}

	// ORDER BY is based on arbitrary input column expressions
	return nil, fmt.Errorf("ORDER BY arbitrary input column expressions is not supported")
}

type filterRows struct {
	tx   sql.Transaction
	rows sql.Rows
	cond sql.CExpr
	dest []sql.Value
}

func (fr *filterRows) EvalRef(idx int) sql.Value {
	return fr.dest[idx]
}

func (fr *filterRows) NumColumns() int {
	return fr.rows.NumColumns()
}

func (fr *filterRows) Close() error {
	return fr.rows.Close()
}

func (fr *filterRows) Next(ctx context.Context, dest []sql.Value) error {
	for {
		err := fr.rows.Next(ctx, dest)
		if err != nil {
			return err
		}
		fr.dest = dest
		defer func() {
			fr.dest = nil
		}()
		v, err := fr.cond.Eval(ctx, fr.tx, fr)
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

func (fr *filterRows) Delete(ctx context.Context) error {
	return fr.rows.Delete(ctx)
}

func (fr *filterRows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	return fr.rows.Update(ctx, updates)
}

func where(ctx context.Context, pctx evaluate.PlanContext, tx sql.Transaction, rop rowsOp,
	fctx *fromContext, cond expr.Expr) (rowsOp, error) {

	if cond == nil {
		return rop, nil
	}
	ce, err := expr.Compile(ctx, pctx, tx, fctx, cond)
	if err != nil {
		return nil, err
	}
	return &filterOp{rop, ce}, nil
}

type filterOp struct {
	rop  rowsOp
	cond sql.CExpr
}

func (_ filterOp) Name() string {
	return "filter"
}

func (fo filterOp) Columns() []string {
	return fo.rop.Columns()
}

func (fo filterOp) Fields() []evaluate.FieldDescription {
	return []evaluate.FieldDescription{
		{Field: "expr", Description: fo.cond.String()},
	}
}

func (fo filterOp) Children() []evaluate.ExplainTree {
	return []evaluate.ExplainTree{fo.rop}
}

func (fo filterOp) rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	r, err := fo.rop.rows(ctx, tx)
	if err != nil {
		return nil, err
	}

	return &filterRows{tx: tx, rows: r, cond: fo.cond}, nil
}

type oneEmptyOp struct{}

func (_ oneEmptyOp) Name() string {
	return "empty"
}

func (_ oneEmptyOp) Columns() []string {
	return nil
}

func (_ oneEmptyOp) Fields() []evaluate.FieldDescription {
	return nil
}

func (_ oneEmptyOp) Children() []evaluate.ExplainTree {
	return nil
}

func (_ oneEmptyOp) rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	return &oneEmptyRow{}, nil
}

type oneEmptyRow struct {
	one bool
}

func (oer *oneEmptyRow) NumColumns() int {
	return 0
}

func (oer *oneEmptyRow) Close() error {
	oer.one = true
	return nil
}

func (oer *oneEmptyRow) Next(ctx context.Context, dest []sql.Value) error {
	if oer.one {
		return io.EOF
	}
	oer.one = true
	return nil
}

func (_ *oneEmptyRow) Delete(ctx context.Context) error {
	return fmt.Errorf("one empty row may not be deleted")
}

func (_ *oneEmptyRow) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	return fmt.Errorf("one empty row may not be updated")
}

type allResultsOp struct {
	rop  rowsOp
	cols []sql.Identifier
}

func (_ *allResultsOp) Name() string {
	return "select"
}

func (aro *allResultsOp) Columns() []string {
	var cols []string
	for _, col := range aro.cols {
		cols = append(cols, col.String())
	}
	return cols
}

func (_ *allResultsOp) Fields() []evaluate.FieldDescription {
	return nil
}

func (aro *allResultsOp) Children() []evaluate.ExplainTree {
	return []evaluate.ExplainTree{aro.rop}
}

func (aro *allResultsOp) rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	r, err := aro.rop.rows(ctx, tx)
	if err != nil {
		return nil, err
	}

	return &allResultRows{r, len(aro.cols)}, nil
}

func (aro *allResultsOp) columns() []sql.Identifier {
	return aro.cols
}

type allResultRows struct {
	rows    sql.Rows
	numCols int
}

func (arr *allResultRows) NumColumns() int {
	return arr.numCols
}

func (arr *allResultRows) Close() error {
	return arr.rows.Close()
}

func (arr *allResultRows) Next(ctx context.Context, dest []sql.Value) error {
	return arr.rows.Next(ctx, dest)
}

func (_ *allResultRows) Delete(ctx context.Context) error {
	return fmt.Errorf("all result rows may not be deleted")
}

func (_ *allResultRows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	return fmt.Errorf("all result rows may not be updated")
}

type resultsOp struct {
	rop       rowsOp
	cols      []sql.Identifier
	destCols  []src2dest
	destExprs []expr2dest
}

func (_ *resultsOp) Name() string {
	return "select"
}

func (ro *resultsOp) Columns() []string {
	var cols []string
	for _, col := range ro.cols {
		cols = append(cols, col.String())
	}
	return cols
}

func (ro *resultsOp) Fields() []evaluate.FieldDescription {
	var fd []evaluate.FieldDescription
	for _, de := range ro.destExprs {
		fd = append(fd,
			evaluate.FieldDescription{
				Field:       "expr",
				Description: fmt.Sprintf("%s = %s", ro.cols[de.destColIndex], de.expr),
			})
	}
	return fd
}

func (ro *resultsOp) Children() []evaluate.ExplainTree {
	return []evaluate.ExplainTree{ro.rop}
}

func (ro *resultsOp) rows(ctx context.Context, tx sql.Transaction) (sql.Rows, error) {
	r, err := ro.rop.rows(ctx, tx)
	if err != nil {
		return nil, err
	}

	return &resultRows{
		tx:        tx,
		rows:      r,
		numCols:   len(ro.cols),
		destCols:  ro.destCols,
		destExprs: ro.destExprs,
	}, nil
}

func (ro *resultsOp) columns() []sql.Identifier {
	return ro.cols
}

type src2dest struct {
	destColIndex int
	srcColIndex  int
}

type expr2dest struct {
	destColIndex int
	expr         sql.CExpr
}

type resultRows struct {
	tx        sql.Transaction
	rows      sql.Rows
	dest      []sql.Value
	numCols   int
	destCols  []src2dest
	destExprs []expr2dest
}

func (rr *resultRows) EvalRef(idx int) sql.Value {
	return rr.dest[idx]
}

func (rr *resultRows) NumColumns() int {
	return rr.numCols
}

func (rr *resultRows) Close() error {
	return rr.rows.Close()
}

func (rr *resultRows) Next(ctx context.Context, dest []sql.Value) error {
	if rr.dest == nil {
		rr.dest = make([]sql.Value, rr.rows.NumColumns())
	}
	err := rr.rows.Next(ctx, rr.dest)
	if err != nil {
		return err
	}
	for _, c2d := range rr.destCols {
		dest[c2d.destColIndex] = rr.dest[c2d.srcColIndex]
	}
	for _, e2d := range rr.destExprs {
		val, err := e2d.expr.Eval(ctx, rr.tx, rr)
		if err != nil {
			return err
		}
		dest[e2d.destColIndex] = val
	}
	return nil
}

func (_ *resultRows) Delete(ctx context.Context) error {
	return fmt.Errorf("result rows may not be deleted")
}

func (_ *resultRows) Update(ctx context.Context, updates []sql.ColumnUpdate) error {
	return fmt.Errorf("result rows may not be updated")
}

func results(ctx context.Context, pctx evaluate.PlanContext, tx sql.Transaction, rop rowsOp,
	fctx *fromContext, results []SelectResult) (resultRowsOp, error) {

	if results == nil {
		return &allResultsOp{rop: rop, cols: fctx.columns()}, nil
	}

	var destExprs []expr2dest
	var cols []sql.Identifier
	ddx := 0
	for _, sr := range results {
		switch sr := sr.(type) {
		case TableResult:
			for _, col := range fctx.tableColumns(sr.Table) {
				ce, err := expr.Compile(ctx, pctx, tx, fctx, expr.Ref{sr.Table, col})
				if err != nil {
					panic(err)
				}
				destExprs = append(destExprs, expr2dest{destColIndex: ddx, expr: ce})
				cols = append(cols, col)
				ddx += 1
			}
		case ExprResult:
			ce, err := expr.Compile(ctx, pctx, tx, fctx, sr.Expr)
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
	return makeResultsOp(rop, cols, destExprs), nil
}

func makeResultsOp(rop rowsOp, cols []sql.Identifier, destExprs []expr2dest) resultRowsOp {
	ro := resultsOp{rop: rop, cols: cols}
	for _, de := range destExprs {
		if ci, ok := expr.ColumnIndex(de.expr); ok {
			ro.destCols = append(ro.destCols,
				src2dest{destColIndex: de.destColIndex, srcColIndex: ci})
		} else {
			ro.destExprs = append(ro.destExprs, de)
		}
	}
	return &ro
}
