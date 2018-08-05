package main

/*
To Do:
- databases should be standalone ==> identifiers should be converted back to strings on storage
- update t.Errorf to be "Operation(args) got %s want %s" and use %q for args
- or "Operation(args) failed with %s" or "Operation(args) did not fail"

- fuzzing: parser.Parse

- ALTER TABLE ...
- memrows: tableImpl: add versioned metadata and use METADATA_MODIFY locking level

- track sessions and transactions; maybe just one table
- server: ssh interactive access
-- table for users + password --or-- authorized public key
-- load authorized public keys from authorized_keys file (same format as used by OpenSSH)
-- -ssh-authorized-keys [./authorized_keys]
-- -authenticate: none, password, public-key (multiple)
- server: logging
-- log (ssh) authorization

- memrows engine: persistence
- memcols engine (w/ mvcc)
- distributed memrows and/or memcols engine, using raft
- boltdb engine
- badger engine
*/

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/engine/memrows"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/server"
	"github.com/leftmike/maho/sql"
)

func replSQL(mgr *engine.Manager, rr io.RuneReader, fn string, w io.Writer, prompt string) {
	p := parser.NewParser(rr, fn)
	ses := server.NewSession(mgr, *eng, sql.ID(*database))
	for {
		if prompt != "" {
			io.WriteString(w, prompt)
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
			func(tx *engine.Transaction, stmt parser.Stmt) error {
				ret, err2 := stmt.Plan(ses, tx)
				if err2 != nil {
					return err2
				}

				if exec, ok := ret.(evaluate.Executor); ok {
					var cnt int64
					cnt, err2 = exec.Execute(ses, tx)
					if err2 != nil {
						return err2
					}
					if cnt >= 0 {
						fmt.Fprintf(w, "%d rows updated\n", cnt)
					}
				} else if rows, ok := ret.(evaluate.Rows); ok {
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

const (
	prompt = "maho> "
)

var (
	database = config.Var(new(string), "database").Usage("default `database` (maho)").String("maho")
	eng      = config.Var(new(string), "engine").Usage("default `engine` (basic)").String("basic")
	dataDir  = config.Var(new(string), "data_directory").
			Flag("data", "`directory` containing databases (./testdata)").String("testdata")
	sshServer = config.Var(new(bool), "ssh").
			Usage("`flag` to control serving ssh (true)").Bool(true)
	sshPort = config.Var(new(string), "ssh-port").Usage("`port` used to serve ssh (:8241)").
		String(":8241")

	configFile = flag.String("config-file", "", "`file` to load config from (./maho.cfg)")
	noConfig   = flag.Bool("no-config", false, "don't load config file")
	listConfig = flag.Bool("list-config", false, "list config and exit")
	repl       = flag.Bool("repl", false, "`flag` to control the console repl (false)")
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
	var sqlArgs, hostKeys []string
	flag.Var((*stringSlice)(&sqlArgs), "sql", "sql `query` to execute; multiple allowed")
	flag.Var((*stringSlice)(&hostKeys), "ssh-host-key",
		"`file` containing a ssh host key; multiple allowed (./id_rsa)")

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

	mgr := engine.NewManager(*dataDir, map[string]engine.Engine{
		"basic":   basic.Engine{},
		"memrows": memrows.Engine{},
	})

	err := mgr.CreateDatabase(*eng, sql.ID(*database), engine.Options{sql.WAIT: "true"})
	if err != nil {
		fmt.Println(err)
		return
	}

	for idx, arg := range sqlArgs {
		replSQL(mgr, strings.NewReader(arg), fmt.Sprintf("sql arg %d", idx+1), os.Stdout, "")
	}

	args := flag.Args()
	for idx := 0; idx < len(args); idx++ {
		f, err := os.Open(args[idx])
		if err != nil {
			fmt.Println(err)
			return
		}
		replSQL(mgr, bufio.NewReader(f), args[idx], ioutil.Discard, "")
	}

	if *sshServer {
		if len(hostKeys) == 0 {
			hostKeys = []string{"id_rsa"}
		}
		ss, err := server.NewSSHServer(mgr, *sshPort, hostKeys, prompt)
		if err != nil {
			fmt.Println(err)
			return
		}
		serve := func(c *server.Client) {
			replSQL(mgr, c.RuneReader, fmt.Sprintf("%s@%s:%s", c.User, c.Type, c.Addr), c.Writer,
				"")
		}
		if *repl {
			go func() {
				err := ss.ListenAndServe(server.HandlerFunc(serve))
				fmt.Println(err)
			}()
		} else {
			fmt.Println(ss.ListenAndServe(server.HandlerFunc(serve)))
		}
	}

	if *repl || (!*sshServer && len(args) == 0 && len(sqlArgs) == 0) {
		replSQL(mgr, bufio.NewReader(os.Stdin), "<console>", os.Stdout, prompt)
	}
}
