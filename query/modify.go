package query

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/sql"
)

type TableName struct {
	Database sql.Identifier
	Table    sql.Identifier
}

type Delete struct {
	Table TableName
	Where expr.Expr
}

func (tn TableName) String() string {
	if tn.Database == 0 {
		return tn.Table.String()
	}
	return fmt.Sprintf("%s.%s", tn.Database, tn.Table)
}

func (stmt *Delete) String() string {
	s := fmt.Sprintf("DELETE FROM %s", stmt.Table)
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

type deletePlan struct {
	rows db.DeleteRows
}

func (dp *deletePlan) Execute(e *engine.Engine) (int64, error) {
	dest := make([]sql.Value, len(dp.rows.Columns()))
	cnt := int64(0)
	for {
		err := dp.rows.Next(dest)
		if err == io.EOF {
			return cnt, nil
		} else if err != nil {
			return cnt, err
		}
		err = dp.rows.Delete()
		if err != nil {
			return cnt, err
		}
		cnt += 1
	}
}

func (stmt *Delete) Plan(e *engine.Engine) (interface{}, error) {
	db, err := e.LookupDatabase(stmt.Table.Database)
	if err != nil {
		return nil, err
	}
	tbl, err := db.Table(stmt.Table.Table)
	if err != nil {
		return nil, err
	}
	rows, err := tbl.DeleteRows()
	if err != nil {
		return nil, err
	}
	if stmt.Where != nil {
		ce, err := expr.Compile(makeFromContext(stmt.Table.Table, rows.Columns()), stmt.Where,
			false)
		if err != nil {
			return nil, err
		}
		rows = &filterDeleteRows{filterRows{rows: rows, cond: ce}}
	}
	return &deletePlan{rows: rows}, nil
}
