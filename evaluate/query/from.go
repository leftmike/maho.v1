package query

import (
	"fmt"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/sql"
)

type FromItem interface {
	fmt.Stringer
	rows(ses evaluate.Session, tx *engine.Transaction) (evaluate.Rows, *fromContext, error)
}

type FromTableAlias sql.TableAlias

func (fta FromTableAlias) String() string {
	return ((sql.TableAlias)(fta)).String()
}

type engineRows struct {
	engine.Rows
}

func (er engineRows) Columns() []sql.Identifier {
	return er.Rows.Columns()
}

func (er engineRows) Close() error {
	return er.Rows.Close()
}

func (er engineRows) Next(ses evaluate.Session, dest []sql.Value) error {
	return er.Rows.Next(ses, dest)
}

func (er engineRows) Delete(ses evaluate.Session) error {
	return er.Rows.Delete(ses)
}

func (er engineRows) Update(ses evaluate.Session, updates []db.ColumnUpdate) error {
	return er.Rows.Update(ses, updates)
}

func lookupRows(ses evaluate.Session, tx *engine.Transaction, dbname,
	tblname sql.Identifier) (evaluate.Rows, error) {

	if dbname == 0 {
		dbname = ses.DefaultDatabase()
	}
	tbl, err := ses.Manager().LookupTable(ses, tx, dbname, tblname)
	if err != nil {
		return nil, err
	}
	rows, err := tbl.Rows(ses)
	if err != nil {
		return nil, err
	}
	return engineRows{rows}, nil
}

func (fta FromTableAlias) rows(ses evaluate.Session, tx *engine.Transaction) (evaluate.Rows,
	*fromContext, error) {

	rows, err := lookupRows(ses, tx, fta.Database, fta.Table)
	if err != nil {
		return nil, nil, err
	}
	nam := fta.Table
	if fta.Alias != 0 {
		nam = fta.Alias
	}
	return rows, makeFromContext(nam, rows.Columns()), nil
}

type colRef struct {
	table  sql.Identifier
	column sql.Identifier
}

func (cr colRef) String() string {
	if cr.table == 0 {
		return cr.column.String()
	}
	return fmt.Sprintf("%s.%s", cr.table, cr.column)
}

type fromContext struct {
	colMap    map[sql.Identifier]int // less than 0 means the column ambiguous
	colRefMap map[colRef]int
	cols      []colRef
}

func makeFromContext(nam sql.Identifier, cols []sql.Identifier) *fromContext {
	fctx := &fromContext{colMap: map[sql.Identifier]int{}, colRefMap: map[colRef]int{}}
	for idx, col := range cols {
		fctx.colMap[col] = idx
		if nam != 0 {
			fctx.colRefMap[colRef{table: nam, column: col}] = idx
		}
		fctx.cols = append(fctx.cols, colRef{table: nam, column: col})
	}
	return fctx
}

func (fctx *fromContext) copy() *fromContext {
	nctx := fromContext{
		colMap:    map[sql.Identifier]int{},
		colRefMap: map[colRef]int{},
		cols:      fctx.cols,
	}
	for col, idx := range fctx.colMap {
		nctx.colMap[col] = idx
	}
	for cr, idx := range fctx.colRefMap {
		nctx.colRefMap[cr] = idx
	}
	return &nctx
}

func (fctx *fromContext) addColumn(cr colRef) {
	idx := len(fctx.cols)
	fctx.cols = append(fctx.cols, cr)
	if _, ok := fctx.colMap[cr.column]; ok {
		fctx.colMap[cr.column] = -1
	} else {
		fctx.colMap[cr.column] = idx
	}
	if _, ok := fctx.colRefMap[cr]; ok {
		fctx.colRefMap[cr] = -1
	} else {
		fctx.colRefMap[cr] = idx
	}
}

func joinContextsOn(lctx, rctx *fromContext) *fromContext {
	// Create a new fromContext as a copy of the left context.
	fctx := lctx.copy()

	// Merge in the right context.
	for _, cr := range rctx.cols {
		fctx.addColumn(cr)
	}
	return fctx
}

func (fctx *fromContext) usingIndex(col sql.Identifier, side string) (int, error) {
	idx, ok := fctx.colMap[col]
	if !ok {
		return -1, fmt.Errorf("engine: %s not found on %s side of join", col, side)
	}
	if idx < 0 {
		return -1, fmt.Errorf("engine: %s is ambigous on %s side of join", col, side)
	}
	return idx, nil
}

func joinContextsUsing(lctx, rctx *fromContext, useSet map[sql.Identifier]struct{}) (*fromContext,
	[]int) {

	// Create a new fromContext as a copy of the left context.
	fctx := lctx.copy()

	// Merge in the right context, skipping columns in use set.
	src2dest := make([]int, 0, len(rctx.cols)-len(useSet))
	for idx, cr := range rctx.cols {
		if _, ok := useSet[cr.column]; ok {
			continue
		}
		fctx.addColumn(cr)
		src2dest = append(src2dest, idx)
	}
	return fctx, src2dest
}

func (fctx *fromContext) CompileRef(r expr.Ref) (int, error) {
	if len(r) == 1 {
		return fctx.colIndex(r[0], "reference")
	} else if len(r) == 2 {
		return fctx.tblColIndex(r[0], r[1], "reference")
	}
	return -1, fmt.Errorf("engine: %s is not a valid reference", r)
}

func (fctx *fromContext) colIndex(col sql.Identifier, what string) (int, error) {
	idx, ok := fctx.colMap[col]
	if !ok {
		return -1, fmt.Errorf("engine: %s %s not found", what, col)
	}
	if idx < 0 {
		return -1, fmt.Errorf("engine: %s %s is ambiguous", what, col)
	}
	return idx, nil
}

func (fctx *fromContext) tblColIndex(tbl, col sql.Identifier, what string) (int, error) {
	if tbl == 0 {
		return fctx.colIndex(col, what)
	}
	cr := colRef{table: tbl, column: col}
	idx, ok := fctx.colRefMap[cr]
	if !ok {
		return -1, fmt.Errorf("engine: %s %s not found", what, cr.String())
	}
	if idx < 0 {
		return -1, fmt.Errorf("engine: %s %s is ambiguous", what, cr.String())
	}
	return idx, nil
}

func (fctx *fromContext) tableColumns(tbl sql.Identifier) []sql.Identifier {
	var cols []sql.Identifier
	for _, cr := range fctx.cols {
		if cr.table == tbl {
			cols = append(cols, cr.column)
		}
	}
	return cols
}

func (fctx *fromContext) columns() []sql.Identifier {
	var cols []sql.Identifier
	for _, cr := range fctx.cols {
		cols = append(cols, cr.column)
	}
	return cols
}

// TestColumns is for testing.
func (fctx *fromContext) TestColumns() []sql.Identifier {
	return fctx.columns()
}