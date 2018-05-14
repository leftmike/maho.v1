package main

/*
To Do:
- databases should be standalone ==> identifiers should be converted back to strings on storage
- update t.Errorf to be "Operation(args) got %s want %s" and use %q for args
- or "Operation(args) failed with %s" or "Operation(args) did not fail"

- fuzzing: parser.Parse

- memory engine (w/ mvcc)
- distributed memory engine, using raft
- boltdb engine
- badger engine
*/

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/engine"
	_ "github.com/leftmike/maho/engine/basic"
	_ "github.com/leftmike/maho/engine/memory"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/plan"
	"github.com/leftmike/maho/session"
	"github.com/leftmike/maho/sql"
)

func replSQL(p parser.Parser, w io.Writer) {
	var tx engine.Transaction
	for {
		stmt, err := p.Parse()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println(err)
			return
		}

		tx, err = engine.Begin()
		if err != nil {
			fmt.Println(err)
			return
		}

		ctx := session.NewContext(*eng, sql.ID(*database))
		ret, err := stmt.Plan(ctx, tx)
		if err != nil {
			fmt.Println(err)
			break
		}

		if exec, ok := ret.(plan.Executer); ok {
			var cnt int64
			cnt, err = exec.Execute(ctx, tx)
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
				err = rows.Next(ctx, dest)
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
				fmt.Println(err)
				break
			}
		}

		err = tx.Commit(ctx)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	err := tx.Rollback()
	if err != nil {
		fmt.Println(err)
	}
}

var (
	database = config.Var(new(string), "database").Flag("database", "default `database`").
			String("maho")
	eng = config.Var(new(string), "engine").Flag("engine", "default `engine`").String("basic")

	configFile = flag.String("config-file", "", "`file` to load config from")
	noConfig   = flag.Bool("no-config", false, "don't load config file")
	listConfig = flag.Bool("list-config", false, "list config and exit")
)

func main() {
	flag.Parse()
	config.Env()

	if *noConfig == false {
		filename := filepath.Join(".", "maho.cfg")
		if *configFile != "" {
			filename = *configFile
		}
		err := config.Load(filename)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	if *listConfig {
		for _, v := range config.Vars() {
			fmt.Printf("[%s] %s = %s\n", v.By(), v.Name(), v.Val())
		}
		return
	}

	err := engine.CreateDatabase(*eng, sql.ID(*database), engine.Options{sql.WAIT: "true"})
	if err != nil {
		fmt.Println(err)
		return
	}

	args := flag.Args()
	if len(args) == 0 {
		replSQL(parser.NewParser(bufio.NewReader(os.Stdin), "[Stdin]"), os.Stdout)
	} else {
		for idx := 0; idx < len(args); idx++ {
			/*			f, err := os.Open(os.Args[idx])
						if err != nil {
							log.Fatal(err)
						}
						replSQL(e, bufio.NewReader(f), os.Args[idx])*/
			replSQL(parser.NewParser(strings.NewReader(args[idx]),
				fmt.Sprintf("os.Args[%d]", idx)), os.Stdout)
		}
	}
}
