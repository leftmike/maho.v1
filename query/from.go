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
	rows(e *engine.Engine) (db.Rows, fromContext, error)
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
	if fta.Table != fta.Alias {
		s += fmt.Sprintf(" AS %s", fta.Alias)
	}
	return s
}

func (fta FromTableAlias) rows(e *engine.Engine) (db.Rows, fromContext, error) {
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
	return rows, makeFromContext(fta.Alias, rows.Columns()), nil
}

type fromColumn struct {
	valid bool
	index int
}

type colRef struct {
	table  sql.Identifier
	column sql.Identifier
}

type fromContext map[colRef]fromColumn

func makeFromContext(nam sql.Identifier, cols []sql.Identifier) fromContext {
	fctx := fromContext{}
	for idx, col := range cols {
		fc := fromColumn{true, idx}
		if nam != 0 {
			fctx[colRef{table: nam, column: col}] = fc
		}
		fctx[colRef{column: col}] = fc
	}
	return fctx
}

func (fctx fromContext) CompileRef(r expr.Ref) (int, error) {
	var fc fromColumn
	ok := false
	if len(r) == 1 {
		fc, ok = fctx[colRef{column: r[0]}]
	} else if len(r) == 2 {
		fc, ok = fctx[colRef{table: r[0], column: r[1]}]
	}
	if !ok {
		return 0, fmt.Errorf("reference %s not found", r.String())
	}
	if !fc.valid {
		return 0, fmt.Errorf("reference %s is ambiguous", r.String())
	}
	return fc.index, nil
}
