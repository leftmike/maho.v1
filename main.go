package main

/*
To Do:
- databases should be standalone ==> identifiers should be converted back to strings on storage
- update t.Errorf to be "Operation(args) got %s want %s" and use %q for args
- or "Operation(args) failed with %s" or "Operation(args) did not fail"

- fuzzing: parser.Parse

- execute DELETE
- execute UPDATE

- SELECT ... ORDER BY ...

- SELECT ...
    [GROUP BY <column-expr> [, ...] [HAVING <condition>]]
- aggregate functions
-- aggregate function in SELECT list or a GROUP BY forces an aggregate context for SELECT list
-- columns must be mentioned in the GROUP BY or must be within an aggregate function
-- column-expr must be *exactly* the same in the SELECT list and in the HAVING condition
-- aggregate functions must not be nested => simple context within an aggregate function
-- HAVING condition applies to GROUP BY rows, so must be an aggregate context
-- need to special case count(*)
-- algorithm:
for (each rows using FROM and WHERE) {
    break into groups using column-expr in GROUP BY, and saving only the GROUP BY column-expr(s)
    accumulate all aggregate functions in each group
}
// result is a Row set of column-expr(s) and aggregate results
for (each group) {
    if not HAVING condition using aggregate results {
        continue
    }
    return SELECT list using aggregate results
}

*/

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/plan"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/store"
	_ "github.com/leftmike/maho/store/basic"
)

func replSQL(e *engine.Engine, p parser.Parser, w io.Writer) {
	for {
		stmt, err := p.Parse()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
			break
		}

		ret, err := stmt.Plan(e)
		if err != nil {
			fmt.Println(err)
			break
		}

		if exec, ok := ret.(plan.Executer); ok {
			cnt, err := exec.Execute(e)
			if err != nil {
				fmt.Println(err)
				break
			}
			fmt.Printf("%d rows updated\n", cnt)
		} else if rows, ok := ret.(plan.Rows); ok {
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
				err = rows.Next(dest)
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
			w.Flush()
			if err != io.EOF {
				fmt.Printf("error: %s\n", err)
			}
		}
	}
}

func start() (*engine.Engine, error) {
	db, err := store.Open("basic", "maho")
	if err != nil {
		return nil, err
	}

	return engine.Start(db)
}

func main() {
	e, err := start()
	if err != nil {
		fmt.Println(err)
		return
	}

	if len(os.Args) == 1 {
		replSQL(e, parser.NewParser(bufio.NewReader(os.Stdin), "[Stdin]"), os.Stdout)
	} else {
		for idx := 1; idx < len(os.Args); idx++ {
			/*			f, err := os.Open(os.Args[idx])
						if err != nil {
							log.Fatal(err)
						}
						replSQL(e, bufio.NewReader(f), os.Args[idx])*/
			replSQL(e, parser.NewParser(strings.NewReader(os.Args[idx]),
				fmt.Sprintf("os.Args[%d]", idx)), os.Stdout)
		}
	}
}
