package main

/*
To Do:
- databases should be standalone ==> identifiers should be converted back to strings on storage
- update t.Errorf to be "Operation(args) got %s want %s" and use %q for args
- or "Operation(args) failed with %s" or "Operation(args) did not fail"

- fuzzing: parser.Parse

- pluggable engine, which is an implementation of a storage engine; only one engine can be
used at a time
- database is a separate storage instance; a single engine can support multiple databases at
the same time
- some shared infrastructure (that is also pluggable): (fat) lock manager; page cache (manager)
- each database has directory of tables: name and physical location
- table metadata is stored with the table and fixed at creation time
- each table has an id and a location; id is fixed at create time; location is physical location
of the table and it can change
- write a tool to dump a database, maybe a page at a time

- Rows.Delete and Rows.Update should not fail as many places: should be able to propogate on
in most cases
- get rid of Table.DeleteRows and Table.UpdateRows; just use Table.Rows
*/

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/leftmike/maho/engine"
	_ "github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/plan"
	"github.com/leftmike/maho/sql"
)

func replSQL(p parser.Parser, w io.Writer) {
	for {
		stmt, err := p.Parse()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
			break
		}

		ret, err := stmt.Plan()
		if err != nil {
			fmt.Println(err)
			break
		}

		if exec, ok := ret.(plan.Executer); ok {
			cnt, err := exec.Execute()
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

func start() error {
	return engine.Start("basic", "testdata", sql.ID("maho"))
}

func main() {
	err := start()
	if err != nil {
		fmt.Println(err)
		return
	}

	if len(os.Args) == 1 {
		replSQL(parser.NewParser(bufio.NewReader(os.Stdin), "[Stdin]"), os.Stdout)
	} else {
		for idx := 1; idx < len(os.Args); idx++ {
			/*			f, err := os.Open(os.Args[idx])
						if err != nil {
							log.Fatal(err)
						}
						replSQL(e, bufio.NewReader(f), os.Args[idx])*/
			replSQL(parser.NewParser(strings.NewReader(os.Args[idx]),
				fmt.Sprintf("os.Args[%d]", idx)), os.Stdout)
		}
	}
}
