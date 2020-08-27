package test

import (
	"fmt"
	"io"
	"strings"

	"github.com/leftmike/sqltest/sqltestdb"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

type Runner struct {
	Engine   sql.Engine
	Database sql.Identifier
	ses      *evaluate.Session
}

func (run *Runner) RunExec(tst *sqltestdb.Test) (int64, error) {
	if run.ses == nil {
		run.ses = &evaluate.Session{
			Engine:          run.Engine,
			DefaultDatabase: run.Database,
			DefaultSchema:   sql.PUBLIC,
		}
	}

	p := parser.NewParser(strings.NewReader(tst.Test),
		fmt.Sprintf("%s:%d", tst.Filename, tst.LineNumber))
	n := int64(-1)
	for {
		stmt, err := p.Parse()
		if err == io.EOF {
			break
		}
		if err != nil {
			return -1, err
		}

		err = run.ses.Run(stmt,
			func(tx sql.Transaction, stmt evaluate.Stmt) error {
				plan, err := stmt.Plan(run.ses, tx)
				if err != nil {
					return err
				}
				if stmtPlan, ok := plan.(evaluate.StmtPlan); ok {
					n, err = stmtPlan.Execute(run.ses.Context(), run.ses.Engine, tx)
					if err != nil {
						return err
					}
				} else if cmdPlan, ok := plan.(evaluate.CmdPlan); ok {
					err = cmdPlan.Command(run.ses)
					if err != nil {
						return err
					}
				} else {
					panic("expected Executor or Commander")
				}

				return nil
			})
		if err != nil {
			return -1, err
		}
	}
	return n, nil
}

func (run *Runner) RunQuery(tst *sqltestdb.Test) ([]string, [][]string, error) {
	if run.ses == nil {
		run.ses = &evaluate.Session{
			Engine:          run.Engine,
			DefaultDatabase: run.Database,
			DefaultSchema:   sql.PUBLIC,
		}
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
		func(tx sql.Transaction, stmt evaluate.Stmt) error {
			plan, err := stmt.Plan(run.ses, tx)
			if err != nil {
				return err
			}
			rowsPlan, ok := plan.(evaluate.RowsPlan)
			if !ok {
				return fmt.Errorf("%s:%d: expected a query", tst.Filename, tst.LineNumber)
			}
			rows, err := rowsPlan.Rows(run.ses.Context(), run.ses.Engine, tx)
			if err != nil {
				return err
			}

			cols := rows.Columns()
			lenCols := len(cols)
			resultCols = make([]string, 0, lenCols)
			for _, col := range cols {
				resultCols = append(resultCols, col.String())
			}

			dest := make([]sql.Value, lenCols)
			for {
				err = rows.Next(run.ses.Context(), dest)
				if err == io.EOF {
					break
				} else if err != nil {
					return err
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
