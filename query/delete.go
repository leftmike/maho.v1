package query

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/oldeng"
	"github.com/leftmike/maho/sql"
)

type Delete struct {
	Table sql.TableName
	Where expr.Expr
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

func (dp *deletePlan) Execute(e *oldeng.Engine) (int64, error) {
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

func (stmt *Delete) Plan(e *oldeng.Engine) (interface{}, error) {
	dbase, err := e.LookupDatabase(stmt.Table.Database)
	if err != nil {
		return nil, err
	}
	t, err := dbase.Table(stmt.Table.Table)
	if err != nil {
		return nil, err
	}
	tbl, ok := t.(db.TableModify)
	if !ok {
		return nil, fmt.Errorf("engine: table \"%s.%s\" can't be modified", dbase.Name(), t.Name())
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
