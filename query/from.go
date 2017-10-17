package query

import (
	"fmt"

	"maho/db"
	"maho/engine"
	"maho/sql"
)

type fromColumn struct {
	valid     bool
	index     int
	ambiguous []string
}

type fromRows struct {
	rows    db.Rows
	columns map[string]fromColumn
}

type FromItem interface {
	fmt.Stringer
	rows(e *engine.Engine) (*fromRows, error)
}

type FromTableAlias struct {
	Database sql.Identifier
	Table    sql.Identifier
	Alias    sql.Identifier
}

type FromSelect struct {
	Select
	Alias sql.Identifier
}

type FromValues struct {
	Values
	Alias sql.Identifier
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

func (fta FromTableAlias) rows(e *engine.Engine) (*fromRows, error) {
	db, err := e.LookupDatabase(fta.Database)
	if err != nil {
		return nil, err
	}
	tbl, err := db.Table(fta.Table)
	if err != nil {
		return nil, err
	}
	rows, err := tbl.Rows()
	if err != nil {
		return nil, err
	}
	return &fromRows{rows: rows}, nil
}

func (fs FromSelect) String() string {
	s := fmt.Sprintf("(%s)", fs.Select.String())
	if fs.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fs.Alias)
	}
	return s
}

func (fs FromSelect) rows(e *engine.Engine) (*fromRows, error) {
	/*
		XXX
		rows, err := fsa.Stmt.Rows(e)
		if err != nil {
			return nil, err
		}
		return &fromRows{rows: rows}, nil
	*/
	return nil, fmt.Errorf("FromSelect.rows not implemented")
}

func (fv FromValues) String() string {
	s := fmt.Sprintf("(%s)", fv.Values.String())
	if fv.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fv.Alias)
	}
	return s
}

func (fv FromValues) rows(e *engine.Engine) (*fromRows, error) {
	/*
		XXX
		rows, err := fsa.Stmt.Rows(e)
		if err != nil {
			return nil, err
		}
		return &fromRows{rows: rows}, nil
	*/
	return nil, fmt.Errorf("FromValues.rows not implemented")
}
