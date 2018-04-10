package main

/*
To Do:
- databases should be standalone ==> identifiers should be converted back to strings on storage
- update t.Errorf to be "Operation(args) got %s want %s" and use %q for args
- or "Operation(args) failed with %s" or "Operation(args) did not fail"

- fuzzing: parser.Parse

- SET param { TO | = } value
*/

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/engine"
	_ "github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/plan"
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

		ctx := context.Background()
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

func start(typ, dataDir string) error {
	return engine.Start(typ, dataDir, sql.ID("maho"))
}

func main() {
	var dataDir string
	var pageSize int // XXX: move to mvcc

	flag.StringVar(&dataDir, "D", "testdata", "`directory` containing database(s)")
	config.StringParam(&dataDir, "data-directory", "testdata",
		config.NoConfigFile|config.NoUpdate)
	config.IntParam(&pageSize, "page-size", 1024*16, config.NoUpdate)

	var boolParam bool
	var durationParam time.Duration
	var float64Param float64
	var intParam int
	var int64Param int64
	var stringParam string
	var uintParam uint
	var uint64Param uint64
	config.BoolParam(&boolParam, "bool-param", false, config.Default)
	config.DurationParam(&durationParam, "duration-param", 0, config.Default)
	config.Float64Param(&float64Param, "float64-param", 1.0, config.Default)
	config.IntParam(&intParam, "int-param", 0, config.Default)
	config.Int64Param(&int64Param, "int64-param", 0, config.Default)
	config.StringParam(&stringParam, "string-param", "default", config.Default)
	config.UintParam(&uintParam, "uint-param", 0, config.Default)
	config.Uint64Param(&uint64Param, "uint64-param", 0, config.Default)

	config.Flags("c", "no-config", "config-file", "config")
	flag.Parse()
	err := config.Load(filepath.Join(dataDir, "maho.cfg"))
	if err != nil {
		fmt.Println(err)
		return
	}

	err = start("basic", dataDir)
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
