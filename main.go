package main

/*
To Do:
- databases should be standalone ==> identifiers should be converted back to strings on storage
- update t.Errorf to be "Operation(args) got %s want %s" and use %q for args
- or "Operation(args) failed with %s" or "Operation(args) did not fail"

- fuzzing: parser.Parse

- ALTER TABLE ...
- memrows: tableImpl: add versioned metadata and use METADATA_MODIFY locking level

- serialize access to config variables

- track sessions and transactions; maybe just one table
- improve interactive execution: prompt, interactive editing (client app?), multiple sessions
- server: ssh interactive access
-- table for users + password --or-- authorized public key
-- load authorized public keys from authorized_keys file (same format as used by OpenSSH)
-- -ssh [true], -ssh-port [8261]
-- -repl [true], -sql (multiple)
-- -ssh-host-key [./id_rsa] (multiple), -ssh-authorized-keys [./authorized_keys]
- server: logging
-- log ssh authorization

- memrows engine: persistence
- memcols engine (w/ mvcc)
- distributed memrows and/or memcols engine, using raft
- boltdb engine
- badger engine

- godoc -http=:6060

- add evaluate.Rows for use internally in evaluate
- db.ColumnType ==> sql.ColumnType; uses expr.Expr, so would need to move that as well
- db.ColumnUpdate ==> sql.ColumnUpdate
- move maho/execute to maho/evaluate
- move Stmt into parser ==> maybe can't do that
- move Executor into server (maybe call evaluator instead) or into evaluate?
- move Session into engine (as interface) and into evaluate (as interface)

- layers: parser / evaluate / engine
- server: ReplSQL, ssh, etc
- main: tie it all together

- fatlock: no global variables

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
	"github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/engine/memrows"
	"github.com/leftmike/maho/execute"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

func replSQL(mgr *engine.Manager, p parser.Parser, w io.Writer) {
	ses := execute.NewSession(mgr, *eng, sql.ID(*database))
	for {
		stmt, err := p.Parse()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Fprintln(w, err)
			continue
		}

		err = ses.Run(stmt,
			func(tx *engine.Transaction, stmt execute.Stmt) error {
				ret, err2 := stmt.Plan(ses, tx)
				if err2 != nil {
					return err2
				}

				if exec, ok := ret.(execute.Executor); ok {
					var cnt int64
					cnt, err2 = exec.Execute(ses, tx)
					if err2 != nil {
						return err2
					}
					if cnt >= 0 {
						fmt.Fprintf(w, "%d rows updated\n", cnt)
					}
				} else if rows, ok := ret.(execute.Rows); ok {
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
						err2 = rows.Next(ses, dest)
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

var (
	database = config.Var(new(string), "database").Usage("default `database`").String("maho")
	eng = config.Var(new(string), "engine").Usage("default `engine`").String("basic")
	dataDir = config.Var(new(string), "data_directory").
		Flag("data", "`directory` containing databases").NoConfig().String("testdata")

	configFile = flag.String("config-file", "", "`file` to load config from")
	noConfig   = flag.Bool("no-config", false, "don't load config file")
	listConfig = flag.Bool("list-config", false, "list config and exit")
)

type stringSlice []string

func (ss *stringSlice) Set(s string) error {
	*ss = append(*ss, s)
	return nil
}

func (ss *stringSlice) String() string {
	return fmt.Sprintf("%v", *ss)
}

func main() {
	var sqlArgs stringSlice
	flag.Var(&sqlArgs, "sql","sql `query` to execute (may be specified more than once)")

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

	mgr := engine.NewManager(map[string]engine.Engine{
		"basic": basic.Engine{},
		"memrows": memrows.Engine{},
	})

	err := mgr.CreateDatabase(*eng, sql.ID(*database), engine.Options{sql.WAIT: "true"})
	if err != nil {
		fmt.Println(err)
		return
	}

	for idx, arg := range sqlArgs {
		replSQL(mgr, parser.NewParser(strings.NewReader(arg), fmt.Sprintf("sql arg %d", idx + 1)),
			os.Stdout)
	}

	args := flag.Args()
	if len(args) == 0 {
		replSQL(mgr, parser.NewParser(bufio.NewReader(os.Stdin), "[Stdin]"), os.Stdout)
	} else {
		for idx := 0; idx < len(args); idx++ {
			/*			f, err := os.Open(os.Args[idx])
						if err != nil {
							log.Fatal(err)
						}
						replSQL(e, bufio.NewReader(f), os.Args[idx])*/
			replSQL(mgr, parser.NewParser(strings.NewReader(args[idx]),
				fmt.Sprintf("os.Args[%d]", idx)), os.Stdout)
		}
	}
}
