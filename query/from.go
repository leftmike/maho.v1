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

type fromContext map[expr.Ref]fromColumn

func makeFromContext(nam sql.Identifier, cols []sql.Identifier) fromContext {
	fctx := fromContext{}
	for idx, col := range cols {
		fc := fromColumn{true, idx}
		if nam != 0 {
			fctx[expr.Ref{nam, col}] = fc
		}
		fctx[expr.Ref{col}] = fc
	}
	return fctx
}

func (fctx fromContext) CompileRef(r expr.Ref) (int, error) {
	fc, ok := fctx[r]
	if !ok {
		return 0, fmt.Errorf("reference %s not found", r.String())
	}
	if !fc.valid {
		return 0, fmt.Errorf("reference %s is ambiguous", r.String())
	}
	return fc.index, nil
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

func where(rows db.Rows, fctx fromContext, cond expr.Expr) (db.Rows, error) {
	ce, err := expr.Compile(fctx, cond)
	if err != nil {
		return nil, err
	}
	return &whereRows{rows: rows, cond: ce}, nil
}
