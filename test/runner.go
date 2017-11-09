package test

import (
	"io"
	"strings"

	"sqltest"

	"maho/db"
	"maho/engine"
	"maho/parser"
	"maho/sql"
)

type Runner struct {
	Engine *engine.Engine
}

func (run *Runner) RunExec(tst *sqltest.Test) error {
	// XXX: include test name in the test
	p := parser.NewParser(strings.NewReader(strings.Join(tst.Stmts, " ")), "XXX")
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

	return nil, nil, nil
}

// AllRows returns all of the rows from a db.Rows as slices of values.
func AllRows(rows db.Rows) ([][]sql.Value, error) {
	all := [][]sql.Value{}
	l := len(rows.Columns())
	for {
		dest := make([]sql.Value, l)
		err := rows.Next(dest)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		all = append(all, dest)
	}
	return all, nil
}
