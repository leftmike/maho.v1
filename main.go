package main

/*
To Do:
- databases should be standalone ==> identifiers should be converted back to strings on storage
- update t.Errorf to be "Operation(args) got %s want %s" and use %q for args
- or "Operation(args) failed with %s" or "Operation(args) did not fail"

- execute DELETE
- execute UPDATE

- test SELECT
-- more general test framework that leverages:
--- function to convert db.Rows into SELECT * FROM (VALUES ...) AS tbl (c1, ...)
--- function to check two db.Rows are equivalent, optionally ignoring ordering
--- some file format to compare the results of some sql stmts with one or more
    SELECT * FROM (VALUES ...) ...
--- go test will ignore directory called testdata

- SELECT ... ORDER BY ...
- joins
- , => CROSS JOIN => INNER JOIN ON (TRUE); is CrossJoin necessary?
- FromJoin.Rows()

- split out display info from ColumnType
*/

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"maho/db"
	"maho/engine"
	"maho/parser"
	"maho/sql"
	"maho/store"
	_ "maho/store/basic"
)

func parse(e *engine.Engine, p parser.Parser, w io.Writer) {
	for {
		stmt, err := p.Parse()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
			break
		}

		ret, err := stmt.Execute(e)
		if err != nil {
			fmt.Println(err)
			break
		}

		if rows, ok := ret.(db.Rows); ok {
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
		parse(e, parser.NewParser(bufio.NewReader(os.Stdin), "[Stdin]"), os.Stdout)
	} else {
		for idx := 1; idx < len(os.Args); idx++ {
			/*			f, err := os.Open(os.Args[idx])
						if err != nil {
							log.Fatal(err)
						}
						parse(e, bufio.NewReader(f), os.Args[idx])*/
			parse(e, parser.NewParser(strings.NewReader(os.Args[idx]), fmt.Sprintf("os.Args[%d]",
				idx)), os.Stdout)
		}
	}
}
