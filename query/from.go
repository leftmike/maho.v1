package query

import (
	"fmt"

	"maho/db"
	"maho/engine"
	"maho/sql"
)

type FromItem interface {
	fmt.Stringer
	Rows(e *engine.Engine) (db.Rows, error)
}

type FromTableAlias struct {
	Database sql.Identifier
	Table    sql.Identifier
	Alias    sql.Identifier
}

type FromStmt struct {
	Stmt  FromItem
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

func (fta FromTableAlias) Rows(e *engine.Engine) (db.Rows, error) {
	db, err := e.LookupDatabase(fta.Database)
	if err != nil {
		return nil, err
	}
	tbl, err := db.Table(fta.Table)
	if err != nil {
		return nil, err
	}

	return tbl.Rows()
}

func (fs FromStmt) String() string {
	s := fmt.Sprintf("(%s)", fs.Stmt.String())
	if fs.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fs.Alias)
	}
	return s
}

func (fs FromStmt) Rows(e *engine.Engine) (db.Rows, error) {
	return fs.Stmt.Rows(e)
}
