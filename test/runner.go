package test

import (
	"fmt"
	"io"
	"strings"

	"github.com/leftmike/sqltest/pkg/sqltest"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/execute"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

type Runner struct {
	Type     string
	Database sql.Identifier
	Mgr      *engine.Manager
	ses      *execute.Session
}

func (run *Runner) RunExec(tst *sqltest.Test) error {
	if run.ses == nil {
		run.ses = execute.NewSession(run.Mgr, run.Type, run.Database)
	}
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
		err = run.ses.Run(stmt,
			func(tx *engine.Transaction, stmt execute.Stmt) error {
				ret, err2 := stmt.Plan(run.ses, tx)
				if err2 != nil {
					return err2
				}
				_, err2 = ret.(execute.Executor).Execute(run.ses, tx)
				if err2 != nil {
					return err2
				}

				return nil
			})

		if err != nil {
			return err
		}
	}
	return nil
}

func (run *Runner) RunQuery(tst *sqltest.Test) ([]string, [][]string, error) {
	if run.ses == nil {
		run.ses = execute.NewSession(run.Mgr, run.Type, run.Database)
	}
	p := parser.NewParser(strings.NewReader(tst.Test),
		fmt.Sprintf("%s:%d", tst.Filename, tst.LineNumber))
	stmt, err := p.Parse()
	if err != nil {
		return nil, nil, err
	}

	var resultCols []string
	var results [][]string
	err = run.ses.Run(stmt,
		func(tx *engine.Transaction, stmt execute.Stmt) error {
			ret, err2 := stmt.Plan(run.ses, tx)
			if err2 != nil {
				return err2
			}
			rows, ok := ret.(execute.Rows)
			if !ok {
				return fmt.Errorf("%s:%d: expected a query", tst.Filename, tst.LineNumber)
			}

			cols := rows.Columns()
			lenCols := len(cols)
			resultCols = make([]string, 0, lenCols)
			for _, col := range cols {
				resultCols = append(resultCols, col.String())
			}

			dest := make([]sql.Value, lenCols)
			for {
				err2 = rows.Next(run.ses, dest)
				if err2 == io.EOF {
					break
				} else if err2 != nil {
					return err2
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

			return nil
		})

	if err != nil {
		return nil, nil, err
	}
	return resultCols, results, nil
}
