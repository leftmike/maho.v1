package main

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

func replSQL(ses *evaluate.Session, p parser.Parser, w io.Writer) {
	for {
		if ses.Interactive {
			io.WriteString(w, "maho> ")
		}

		stmt, err := p.Parse()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Fprintln(w, err)
			continue
		}

		err = ses.Run(stmt,
			func(tx sql.Transaction, stmt evaluate.Stmt) error {
				plan, err := stmt.Plan(ses, ses.Context(), ses.Engine, tx)
				if err != nil {
					return err
				}

				if stmtPlan, ok := plan.(evaluate.StmtPlan); ok {
					var cnt int64
					cnt, err = stmtPlan.Execute(ses.Context(), ses.Engine, tx)
					if err != nil {
						return err
					}
					if cnt >= 0 {
						fmt.Fprintf(w, "%d rows updated\n", cnt)
					}
				} else if cmdPlan, ok := plan.(evaluate.CmdPlan); ok {
					err = cmdPlan.Command(ses)
					if err != nil {
						return err
					}
				} else if rowsPlan, ok := plan.(evaluate.RowsPlan); ok {
					w := tabwriter.NewWriter(w, 0, 0, 1, ' ', tabwriter.AlignRight)

					rows, err := rowsPlan.Rows(ses.Context(), ses.Engine, tx)
					if err != nil {
						return err
					}

					cols := rows.Columns()
					fmt.Fprint(w, "\t")
					for _, col := range cols {
						fmt.Fprintf(w, "%s\t", col)
					}
					fmt.Fprint(w, "\n\t")
					for _, col := range cols {
						fmt.Fprintf(w, "%s\t", strings.Repeat("-", len(col.String())))

					}
					fmt.Fprintln(w)

					dest := make([]sql.Value, len(cols))
					i := 1
					for {
						err = rows.Next(ses.Context(), dest)
						if err != nil {
							break
						}
						fmt.Fprintf(w, "%d\t", i)
						for _, v := range dest {
							fmt.Fprintf(w, "%s\t", sql.Format(v))
						}
						fmt.Fprintln(w)
						i += 1
					}
					fmt.Fprintf(w, "(%d rows)\n", i-1)
					w.Flush()
					if err != io.EOF {
						return err
					}
				}
				return nil
			})

		if err != nil {
			fmt.Fprintln(w, err)
		}
	}
}
