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
- each database has tables table (db$tables) which then links to everything else
- each table has an id and a location; id is fixed at create time; location is physical location
of the table and it can change
- Store --> Engine
- write a tool to dump a database, maybe a page at a time

- combine Rows, DeleteRows, and UpdateRows into a single interface (and fix filterRows)
- combine Table and TableModify into a single interface
- remove Table.Name()
- remove db.Database
- sql.TABLES --> DB_TABLES (DB$TABLES)
- sql.COLUMNS --> DB_COLUMNS (DB$COLUMNS)
- remove sql.BASIC
- rename engine/neweng.go --> engine/engine.go
- rm -R store/
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
	"github.com/leftmike/maho/oldeng"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/plan"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/store"
	_ "github.com/leftmike/maho/store/basic"
)

func replSQL(e *oldeng.Engine, p parser.Parser, w io.Writer) {
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

func start() (*oldeng.Engine, error) {
	err := engine.Start("basic", "testdata", "maho")
	if err != nil {
		fmt.Printf("engine.Start: %s\n", err)
	}

	db, err := store.Open("basic", "maho")
	if err != nil {
		return nil, err
	}

	return oldeng.Start(db)
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
