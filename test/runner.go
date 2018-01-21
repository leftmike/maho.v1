package test

import (
	"fmt"
	"io"
	"strings"

	"github.com/leftmike/sqltest/pkg/sqltest"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

type Runner struct {
	Engine *engine.Engine
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
		_, err = stmt.Execute(run.Engine)
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
	ret, err := stmt.Execute(run.Engine)
	if err != nil {
		return nil, nil, err
	}
	rows, ok := ret.(db.Rows)
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
		err := rows.Next(dest)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, err
		}
		row := make([]string, 0, lenCols)
		for _, v := range dest {
			if v == nil {
				row = append(row, "")
			} else if s, ok := v.(string); ok {
				row = append(row, s)
			} else {
				row = append(row, sql.Format(v))
			}
		}
		results = append(results, row)
	}
	return resultCols, results, nil
}
