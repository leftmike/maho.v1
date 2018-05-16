package test

import (
	"fmt"
	"io"
	"strings"

	"github.com/leftmike/sqltest/pkg/sqltest"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/plan"
	"github.com/leftmike/maho/session"
	"github.com/leftmike/maho/sql"
)

type Runner struct {
	Type     string
	Database sql.Identifier
}

func (run *Runner) RunExec(tst *sqltest.Test) error {
	p := parser.NewParser(strings.NewReader(tst.Test),
		fmt.Sprintf("%s:%d", tst.Filename, tst.LineNumber))
	for {
		stmt, err := p.Parse()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		tx := engine.Begin()
		ctx := session.NewContext(run.Type, run.Database)
		ret, err := stmt.Plan(ctx, tx)
		if err != nil {
			return err
		}
		_, err = ret.(plan.Executer).Execute(ctx, tx)
		if err != nil {
			return err
		}
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (run *Runner) RunQuery(tst *sqltest.Test) ([]string, [][]string, error) {
	p := parser.NewParser(strings.NewReader(tst.Test),
		fmt.Sprintf("%s:%d", tst.Filename, tst.LineNumber))
	stmt, err := p.Parse()
	if err != nil {
		return nil, nil, err
	}
	tx := engine.Begin()
	ctx := session.NewContext(run.Type, run.Database)
	ret, err := stmt.Plan(ctx, tx)
	if err != nil {
		return nil, nil, err
	}
	rows, ok := ret.(plan.Rows)
	if !ok {
		return nil, nil, fmt.Errorf("%s:%d: expected a query", tst.Filename, tst.LineNumber)
	}

	cols := rows.Columns()
	lenCols := len(cols)
	resultCols := make([]string, 0, lenCols)
	for _, col := range cols {
		resultCols = append(resultCols, col.String())
	}

	var results [][]string
	dest := make([]sql.Value, lenCols)
	for {
		err = rows.Next(ctx, dest)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, err
		}
		row := make([]string, 0, lenCols)
		for _, v := range dest {
			if v == nil {
				row = append(row, "")
			} else if s, ok := v.(sql.StringValue); ok {
				row = append(row, string(s))
			} else {
				row = append(row, sql.Format(v))
			}
		}
		results = append(results, row)
	}
	err = tx.Commit(ctx)
	if err != nil {
		return nil, nil, err
	}
	return resultCols, results, nil
}
