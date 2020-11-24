package test

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/leftmike/sqltest/sqltestdb"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

type Runner evaluate.Session

func (run *Runner) RunExec(tst *sqltestdb.Test) (int64, error) {
	ses := ((*evaluate.Session)(run))

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

		err = ses.Run(stmt,
			func(ctx context.Context, ses *evaluate.Session, e sql.Engine,
				tx sql.Transaction) error {

				plan, err := stmt.Plan(ctx, ses, tx)
				if err != nil {
					return err
				}
				if stmtPlan, ok := plan.(evaluate.StmtPlan); ok {
					n, err = stmtPlan.Execute(ctx, tx)
					if err != nil {
						return err
					}
				} else if cmdPlan, ok := plan.(evaluate.CmdPlan); ok {
					err = cmdPlan.Command(ctx, ses, e)
					if err != nil {
						return err
					}
				} else {
					return fmt.Errorf("%s:%d: expected a stmt or cmd", tst.Filename,
						tst.LineNumber)
				}

				return nil
			})
		if err != nil {
			return -1, err
		}
	}
	return n, nil
}

func columnType(ct sql.ColumnType) string {
	switch ct.Type {
	case sql.UnknownType, sql.BooleanType:
		return ct.Type.String()
	case sql.BytesType:
		return "BYTEA"
	case sql.StringType:
		if ct.Fixed {
			return "BPCHAR"
		} else if ct.Size < sql.MaxColumnSize {
			return "VARCHAR"
		} else {
			return "TEXT"
		}
	case sql.FloatType:
		return fmt.Sprintf("FLOAT%d", ct.Size)
	case sql.IntegerType:
		return fmt.Sprintf("INT%d", ct.Size)
	default:
		panic(fmt.Sprintf("unexpected column type; got %#v", ct.Type))
	}
}

func (run *Runner) RunQuery(tst *sqltestdb.Test) (sqltestdb.QueryResult, error) {
	ses := ((*evaluate.Session)(run))

	p := parser.NewParser(strings.NewReader(tst.Test),
		fmt.Sprintf("%s:%d", tst.Filename, tst.LineNumber))
	stmt, err := p.Parse()
	if err != nil {
		return sqltestdb.QueryResult{}, err
	}

	var resultCols []string
	var results [][]string
	var resultTypes []string
	err = ses.Run(stmt,
		func(ctx context.Context, ses *evaluate.Session, e sql.Engine,
			tx sql.Transaction) error {

			plan, err := stmt.Plan(ctx, ses, tx)
			if err != nil {
				return err
			}
			rowsPlan, ok := plan.(evaluate.RowsPlan)
			if !ok {
				return fmt.Errorf("%s:%d: expected a query", tst.Filename, tst.LineNumber)
			}
			rows, err := rowsPlan.Rows(ctx, tx)
			if err != nil {
				return err
			}

			cols := rowsPlan.Columns()
			lenCols := len(cols)
			resultCols = make([]string, 0, lenCols)
			for cdx := range cols {
				resultCols = append(resultCols, cols[cdx].String())
			}

			for _, ct := range rowsPlan.ColumnTypes() {
				resultTypes = append(resultTypes, columnType(ct))
			}

			dest := make([]sql.Value, lenCols)
			for {
				err = rows.Next(ctx, dest)
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
					} else if b, ok := v.(sql.BytesValue); ok {
						row = append(row, string(b))
					} else {
						row = append(row, sql.Format(v))
					}
				}
				results = append(results, row)
			}

			return nil
		})

	if err != nil {
		return sqltestdb.QueryResult{}, err
	}
	return sqltestdb.QueryResult{
		Columns:   resultCols,
		TypeNames: resultTypes,
		Rows:      results,
	}, nil
}
