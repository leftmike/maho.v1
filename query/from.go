package query

import (
	"fmt"

	"maho/db"
	"maho/engine"
	"maho/expr"
	"maho/sql"
)

type FromItem interface {
	fmt.Stringer
	rows(e *engine.Engine) (db.Rows, *fromContext, error)
}

type FromTableAlias struct {
	Database sql.Identifier
	Table    sql.Identifier
	Alias    sql.Identifier
}

func (fta FromTableAlias) String() string {
	var s string
	if fta.Database == 0 {
		s = fta.Table.String()
	} else {
		s = fmt.Sprintf("%s.%s", fta.Database, fta.Table)
	}
	if fta.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fta.Alias)
	}
	return s
}

func (fta FromTableAlias) rows(e *engine.Engine) (db.Rows, *fromContext, error) {
	db, err := e.LookupDatabase(fta.Database)
	if err != nil {
		return nil, nil, err
	}
	tbl, err := db.Table(fta.Table)
	if err != nil {
		return nil, nil, err
	}
	rows, err := tbl.Rows()
	if err != nil {
		return nil, nil, err
	}
	nam := fta.Table
	if fta.Alias != 0 {
		nam = fta.Alias
	}
	return rows, makeFromContext(nam, rows.Columns()), nil
}

type fromColumn struct {
	valid bool
	index int
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

type colIndex struct {
	colRef
	index int
}

type fromContext struct {
	colMap map[colRef]fromColumn
	cols   []colIndex
}

func makeFromContext(nam sql.Identifier, cols []sql.Identifier) *fromContext {
	fctx := &fromContext{colMap: map[colRef]fromColumn{}}
	for idx, col := range cols {
		fc := fromColumn{true, idx}
		if nam != 0 {
			fctx.colMap[colRef{table: nam, column: col}] = fc
			fctx.cols = append(fctx.cols, colIndex{colRef{table: nam, column: col}, idx})
		}
		fctx.colMap[colRef{column: col}] = fc
	}
	return fctx
}

func joinContextsOn(lctx, rctx *fromContext) *fromContext {
	// Create a new fromContext as a copy of the left context.
	fctx := &fromContext{colMap: map[colRef]fromColumn{}, cols: lctx.cols}
	for cr, fc := range lctx.colMap {
		fctx.colMap[cr] = fc
	}

	// Merge in the right context, offsetting indexes and marking colRefs as invalid if they
	// refer to more than one column.
	off := len(lctx.cols)
	for cr, fc := range rctx.colMap {
		if efc, ok := fctx.colMap[cr]; ok {
			efc.valid = false
			fctx.colMap[cr] = efc
		} else {
			fc.index += off
			fctx.colMap[cr] = fc
		}
	}
	for _, ci := range rctx.cols {
		ci.index += off
		fctx.cols = append(fctx.cols, ci)
	}

	return fctx
}

func (fctx *fromContext) usingIndex(col sql.Identifier, side string) (int, error) {
	cr := colRef{column: col}
	fc, ok := fctx.colMap[cr]
	if !ok {
		return -1, fmt.Errorf("%s not found on %s side of join", cr.String(), side)
	}
	if !fc.valid {
		return -1, fmt.Errorf("%s is ambigous on %s side of join", cr.String(), side)
	}
	return fc.index, nil
}

func joinContextsUsing(lctx, rctx *fromContext, useSet map[colRef]struct{}) (*fromContext, []int) {
	// Create a new fromContext as a copy of the left context.
	fctx := &fromContext{colMap: map[colRef]fromColumn{}, cols: lctx.cols}
	for cr, fc := range lctx.colMap {
		fctx.colMap[cr] = fc
	}

	// XXX: clean this up: this seems like it is more complex than it needs to be.

	// Merge in the right context: after skipping columns in useSet (they will be included as part
	// of the left context), offset indexes and mark colRefs as invalid if they refer to more than
	// one column.
	off := len(lctx.cols)
	skippedIndexes := map[int]struct{}{}
	for cr, fc := range rctx.colMap {
		if _, ok := useSet[cr]; ok {
			skippedIndexes[fc.index] = struct{}{}
			continue
		}

		if efc, ok := fctx.colMap[cr]; ok {
			efc.valid = false
			fctx.colMap[cr] = efc
		} else {
			fc.index += off
			fctx.colMap[cr] = fc
		}
	}
	for _, ci := range rctx.cols {
		if _, ok := skippedIndexes[ci.index]; ok {
			continue
		}

		ci.index += off
		fctx.cols = append(fctx.cols, ci)
	}
	src2dest := make([]int, len(fctx.cols)-off)
	idx := 0
	for _, ci := range rctx.cols {
		if _, ok := skippedIndexes[ci.index]; ok {
			continue
		}
		src2dest[idx] = ci.index
		idx += 1
	}
	return fctx, src2dest
}

func (fctx *fromContext) CompileRef(r expr.Ref) (int, error) {
	var tbl, col sql.Identifier
	if len(r) == 1 {
		col = r[0]
	} else if len(r) == 2 {
		tbl = r[0]
		col = r[1]
	}
	return fctx.columnIndex(tbl, col, "reference")
}

func (fctx *fromContext) columnIndex(tbl, col sql.Identifier, what string) (int, error) {
	cr := colRef{table: tbl, column: col}
	fc, ok := fctx.colMap[cr]
	if !ok {
		return -1, fmt.Errorf("%s %s not found", what, cr.String())
	}
	if !fc.valid {
		return -1, fmt.Errorf("%s %s is ambiguous", what, cr.String())
	}
	return fc.index, nil
}

func (fctx *fromContext) tableColumns(tbl sql.Identifier) []colIndex {
	var cols []colIndex
	for _, ci := range fctx.cols {
		if ci.table == tbl {
			cols = append(cols, ci)
		}
	}
	return cols
}

func (fctx *fromContext) columns() []sql.Identifier {
	var cols []sql.Identifier
	for _, ci := range fctx.cols {
		cols = append(cols, ci.column)
	}
	return cols
}
