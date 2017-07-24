package main

/*
To Do:
- databases should be standalone ==> identifiers should be converted back to strings on storage
- finish parse and execute of select
- evaluating expressions
- DEFAULT for column can be an expression in CREATE TABLE
- INSERT VALUES can be expressions
- update t.Errorf to be "Operation(args) got %s want %s" and use %q for args
- or "Operation(args) failed with %s" or "Operation(args) did not fail"
- handle DEFAULT specially in INSERT VALUES; it should not be part of value.Format
- is eval.value really necessary?
- add tests to maho and maho/sql/stmt even if just stubs
- eval failure tests
*/

import (
	"bufio"
	"fmt"
	"io"
	"maho/engine"
	"maho/sql"
	"maho/sql/parser"
	"maho/store"
	_ "maho/store/basic"
	"os"
	"strings"
	"text/tabwriter"
)

func parse(e *engine.Engine, rr io.RuneReader, fn string) {
	var p parser.Parser
	p.Init(rr, fn)

	for {
		stmt, err := p.Parse()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
			break
		}

		ret, err := stmt.Dispatch(e)
		if err != nil {
			fmt.Println(err)
			break
		}

		if rows, ok := ret.(store.Rows); ok {
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.AlignRight)

			cols := rows.Columns()
			fmt.Fprint(w, "\t")
			for _, col := range cols {
				fmt.Fprintf(w, "%s\t", col.Name)
			}
			fmt.Fprint(w, "\n\t")
			for _, col := range cols {
				fmt.Fprintf(w, "%s\t", strings.Repeat("-", len(col.Name.String())))

			}
			fmt.Fprintln(w)

			dest := make([]sql.Value, len(cols))
			for i := 1; rows.Next(dest) == nil; i += 1 {
				fmt.Fprintf(w, "%d\t", i)
				for _, v := range dest {
					fmt.Fprintf(w, "%s\t", sql.Format(v))
				}
				fmt.Fprintln(w)
			}
			w.Flush()
		}
	}
}

func main() {
	db, err := store.Open("basic", "maho")
	if err != nil {
		fmt.Println(err)
		return
	}

	e, err := engine.Start(db)
	if err != nil {
		fmt.Println(err)
		return
	}

	if len(os.Args) == 1 {
		parse(e, bufio.NewReader(os.Stdin), "[Stdin]")
	} else {
		for idx := 1; idx < len(os.Args); idx++ {
			/*			f, err := os.Open(os.Args[idx])
						if err != nil {
							log.Fatal(err)
						}
						parse(e, bufio.NewReader(f), os.Args[idx])*/
			parse(e, strings.NewReader(os.Args[idx]), fmt.Sprintf("os.Args[%d]", idx))
		}
	}
}
