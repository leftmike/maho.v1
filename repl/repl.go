package repl

import (
	"context"
	"fmt"
	"io"

	"github.com/olekukonko/tablewriter"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

func ReplSQL(ses *evaluate.Session, p parser.Parser, w io.Writer) {
	for {
		stmt, err := p.Parse()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Fprintln(w, err)
			continue
		}

		err = ses.Run(stmt,
			func(ctx context.Context, ses *evaluate.Session, e sql.Engine,
				tx sql.Transaction) error {

				plan, err := stmt.Plan(ctx, ses, tx, nil)
				if err != nil {
					return err
				}

				if stmtPlan, ok := plan.(evaluate.StmtPlan); ok {
					var cnt int64
					cnt, err = stmtPlan.Execute(ctx, tx)
					if err != nil {
						return err
					}
					if cnt >= 0 {
						fmt.Fprintf(w, "%d rows updated\n", cnt)
					}
				} else if cmdPlan, ok := plan.(evaluate.CmdPlan); ok {
					err = cmdPlan.Command(ctx, ses, e)
					if err != nil {
						return err
					}
				} else if rowsPlan, ok := plan.(evaluate.RowsPlan); ok {
					tw := tablewriter.NewWriter(w)
					tw.SetAutoFormatHeaders(false)

					rows, err := rowsPlan.Rows(ctx, tx, nil)
					if err != nil {
						return err
					}

					cols := rowsPlan.Columns()
					row := make([]string, len(cols))
					for cdx, col := range cols {
						row[cdx] = col.String()
					}
					tw.SetHeader(row)

					dest := make([]sql.Value, len(cols))
					for {
						err = rows.Next(ctx, dest)
						if err != nil {
							break
						}

						for cdx, v := range dest {
							if v != nil {
								if s, ok := v.(sql.StringValue); ok {
									row[cdx] = string(s)
									continue
								}
							}
							row[cdx] = sql.Format(v)
						}
						tw.Append(row)
					}
					tw.Render()
					fmt.Fprintf(w, "(%d rows)\n", tw.NumLines())
					if err != io.EOF {
						return err
					}
				} else {
					panic(fmt.Sprintf("expected StmtPlan, CmdPlan, or RowsPlan: %#v", plan))
				}

				return nil
			})

		if err != nil {
			fmt.Fprintln(w, err)
		}
	}
}

func Handler(rr io.RuneReader, w io.Writer) evaluate.SessionHandler {
	return func(ses *evaluate.Session) {
		src := fmt.Sprintf("%s@%s", ses.User, ses.Type)
		if ses.Addr != "" {
			src = fmt.Sprintf("%s:%s", src, ses.Addr)
		}
		ReplSQL(ses, parser.NewParser(rr, src), w)
	}
}
