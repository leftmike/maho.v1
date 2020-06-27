package main

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/leftmike/maho/engine"
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
			func(tx engine.Transaction, stmt evaluate.Stmt) error {
				ret, err2 := stmt.Plan(ses, tx)
				if err2 != nil {
					return err2
				}

				if exec, ok := ret.(evaluate.Executor); ok {
					var cnt int64
					cnt, err2 = exec.Execute(ses.Context(), ses.Engine, tx)
					if err2 != nil {
						return err2
					}
					if cnt >= 0 {
						fmt.Fprintf(w, "%d rows updated\n", cnt)
					}
				} else if cmd, ok := ret.(evaluate.Commander); ok {
					err2 = cmd.Command(ses)
					if err2 != nil {
						return err2
					}
				} else if rows, ok := ret.(sql.Rows); ok {
					w := tabwriter.NewWriter(w, 0, 0, 1, ' ', tabwriter.AlignRight)

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
						err2 = rows.Next(ses.Context(), dest)
						if err2 != nil {
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
					if err2 != io.EOF {
						return err2
					}
				}
				return nil
			})

		if err != nil {
			fmt.Fprintln(w, err)
		}
	}
}
