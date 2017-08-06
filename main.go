package main

/*
To Do:
- databases should be standalone ==> identifiers should be converted back to strings on storage
- row: row.Set is columns in the set and maybe Count of rows; row.Scanner is an interface to
  scan a set of rows; row.Slice is an interface to return the rows as a slice; utility routines
  to convert between the two -- if necessary
- finish parse and execute of select
- references in expressions
- update t.Errorf to be "Operation(args) got %s want %s" and use %q for args
- or "Operation(args) failed with %s" or "Operation(args) did not fail"
- change AliasTableName to TableAlias (and the same for Columns)
- should stmt be objects which parser builds and know how to execute themselves using engine?

p := parser.NewParser(rr io.RuneReader, fn string)
stmt, err := p.Parse()

maho/parser --> expr, sql, stmt
maho/parser/token
maho/parser/scanner
maho/engine
maho/expr --> sql
maho/row
maho/sql
maho/stmt --> sql, expr
maho/store
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

func parse(e *engine.Engine, rr io.RuneReader, fn string, w io.Writer) {
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
			w := tabwriter.NewWriter(w, 0, 0, 1, ' ', tabwriter.AlignRight)

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
		parse(e, bufio.NewReader(os.Stdin), "[Stdin]", os.Stdout)
	} else {
		for idx := 1; idx < len(os.Args); idx++ {
			/*			f, err := os.Open(os.Args[idx])
						if err != nil {
							log.Fatal(err)
						}
						parse(e, bufio.NewReader(f), os.Args[idx])*/
			parse(e, strings.NewReader(os.Args[idx]), fmt.Sprintf("os.Args[%d]", idx), os.Stdout)
		}
	}
}
