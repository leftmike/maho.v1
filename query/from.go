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
		return 0, fmt.Errorf("%s %s not found", what, cr.String())
	}
	if !fc.valid {
		return 0, fmt.Errorf("%s %s is ambiguous", what, cr.String())
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
