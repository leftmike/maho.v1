package main

/*
To Do:
- fuzzing: parser.Parse

- add test for not seeing modified rows within a single SQL statement

- add type sql.ColumnValue interface{} and type BoolColumn []bool, type Int64Column []int64, etc
- Rows.NextColumns(ctx context.Context, destCols []sql.ColumnValue) error

- specify a subset of columns to return: Table.Rows(cols []int, ...)

- use cockroachdb/pebble as a storage engine with kvrows

- use knz/go-libedit for readline
- use spf13/cobra for command argument handling

- use jackc/pgx or cockroach/pkg/sql/pgwire for client sql interface

- make sure all Rows get properly closed

- get rid of engine.ListDatabases: only used at startup in main.go

- storage/service might no longer be necessary?

- tests with 1000s to 100000s of rows
-- generate rows
-- use sample databases

- kvrows
-- cleanup proposals
-- consider making Rows() incremental, maybe as blocks of rows
-- badger: iso-3166.sql: can't select from country
-- badger: usda.sql: can't load largest table; can't select from tables

- rowcols
-- snapshot store and truncate WAL
-- usda.sql: causes corrupt WAL

- subquery expressions: EXISTS, IN, NOT IN, ANY/SOME, ALL
- conditional expressions: CASE, COALESCE, NULLIF, GREATEST, LEAST

- indexes
-- unique indexes: all NULL values are considered different from all other NULL values and
   are thus unique (sqlite.org/lang_createindex.html)
-- based on column numbers
-- engine.Table: IndexRows(...) => engine.IndexRows {Columns, Close, Next, Delete, Update, Row}
-- add unique indexes to metadata.constraints table
-- add primary key to metadata.constraints table

- constraints
-- change unique constraints into indexes
-- column: REFERENCES reftable [ '(' refcolumn ')' ] => treat as a foreign key
-- table: FOREIGN KEY '(' column  [',' ...] ')' REFERENCES reftable [ '(' refcolumn [',' ...] ')' ]
   => list of Foreign on the table (and need a list of Refering tables)
-- drop CHECK constraint: use ALTER TABLE table DROP CONSTRAINT constraint
-- drop DEFAULT: use ALTER TABLE table ALTER COLUMN column DROP DEFAULT
-- drop FOREIGN KEY: use ALTER TABLE table DROP CONSTRAINT constraint
-- drop NOT NULL: use ALTER TABLE table ALTER COLUMN column DROP NOT NULL
-- drop UNIQUE: use DROP INDEX ...
*/

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/server"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/basic"
	"github.com/leftmike/maho/storage/keyval"
	"github.com/leftmike/maho/storage/rowcols"
)

var (
	database = config.Var(new(string), "database").Usage("default `database` (maho)").String("maho")
	store    = config.Var(new(string), "store").Usage("`store` (basic)").String("basic")
	dataDir  = config.Var(new(string), "data-directory").
			Flag("data", "`directory` containing databases (testdata)").String("testdata")
	sshServer = config.Var(new(bool), "ssh").
			Usage("`flag` to control serving ssh (false)").Bool(false)
	sshPort = config.Var(new(string), "ssh-port").
		Usage("`port` used to serve ssh (localhost:8241)").String("localhost:8241")
	logFile = config.Var(new(string), "log-file").Usage("`file` to use for logging (maho.log)").
		String("maho.log")
	logLevel = config.Var(new(string), "log-level").
			Usage("log level: debug, info, warn, error, fatal, or panic (info)").String("info")
	authorizedKeys = config.Var(new(string), "ssh-authorized-keys").
			Usage("`file` containing authorized ssh keys").String("")

	accounts = config.Var(new(config.Array), "accounts").Array()

	configFile = flag.String("config-file", "", "`file` to load config from (maho.hcl)")
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

func parseAccounts(accounts config.Array) (map[string]string, bool) {
	userPasswords := map[string]string{}
	for _, a := range accounts {
		account, ok := a.(map[string]interface{})
		if !ok {
			return nil, false
		}
		user, ok := account["user"].(string)
		if !ok {
			return nil, false
		}
		password, ok := account["password"].(string)
		if !ok {
			return nil, false
		}
		userPasswords[user] = password
	}

	return userPasswords, true
}

func main() {
	log.SetFormatter(&log.TextFormatter{
		DisableLevelTruncation: true,
	})

	var sqlArgs, hostKeys []string
	flag.Var((*stringSlice)(&sqlArgs), "sql", "sql `query` to execute; multiple allowed")
	flag.Var((*stringSlice)(&hostKeys), "ssh-host-key",
		"`file` containing a ssh host key; multiple allowed (id_rsa)")

	var logStderr bool
	for _, s := range []string{"log-stdout", "s"} {
		flag.BoolVar(&logStderr, s, false, "`flag` to control logging to standard error (false)")
	}

	flag.Parse()
	config.Env()

	if *noConfig == false {
		filename := "maho.hcl"
		if *configFile != "" {
			filename = *configFile
		}
		err := config.Load(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "maho: %s: %s\n", *configFile, err)
			return
		}
	}
	if *listConfig {
		for _, v := range config.Vars() {
			fmt.Fprintf(os.Stdout, "[%s] %s = %s\n", v.By(), v.Name(), v.Val())
		}
		return
	}

	userPasswords, ok := parseAccounts(*accounts)
	if !ok {
		fmt.Fprintf(os.Stderr,
			"maho: %s: expected array of {user: <user>, password: <password>} for accounts\n",
			*configFile)
		return
	}

	if !logStderr && *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			fmt.Fprintf(os.Stderr, "maho: %s\n", err)
			return
		}
		defer f.Close()
		log.SetOutput(f)
	}

	ll, ok := map[string]log.Level{
		"panic": log.PanicLevel,
		"fatal": log.FatalLevel,
		"error": log.ErrorLevel,
		"warn":  log.WarnLevel,
		"info":  log.InfoLevel,
		"debug": log.DebugLevel,
	}[*logLevel]
	if !ok {
		fmt.Fprintf(os.Stderr,
			"maho: got %s for log level; want debug, info, warn, error, fatal, or panic",
			*logLevel)
		return
	}
	log.SetLevel(ll)

	log.WithField("pid", os.Getpid()).Info("maho starting")

	var st *storage.Store
	var err error
	switch *store {
	case "basic":
		st, err = basic.NewStore(*dataDir)
	case "rowcols":
		st, err = rowcols.NewStore(*dataDir)
	case "badger":
		st, err = keyval.NewBadgerStore(*dataDir)
	case "bbolt":
		st, err = keyval.NewBBoltStore(*dataDir)
	default:
		fmt.Fprintf(os.Stderr,
			"maho: got %s for store; want basic, rowcols, badger, or bbolt", *store)
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "maho: %s", err)
		return
	}

	e := engine.NewEngine(st)
	svr := server.Server{
		Handler: func(ses *evaluate.Session, rr io.RuneReader, w io.Writer) {
			src := fmt.Sprintf("%s@%s", ses.User, ses.Type)
			if ses.Addr != "" {
				src = fmt.Sprintf("%s:%s", src, ses.Addr)
			}
			replSQL(ses,
				parser.NewParser(rr, src), w)
		},
		Engine:          e,
		DefaultDatabase: sql.ID(*database),
	}

	tx := e.Begin(0)
	dbs, err := e.ListDatabases(context.Background(), tx)
	tx.Rollback()

	defaultDB := sql.ID(*database)
	var found bool
	for _, db := range dbs {
		if db == defaultDB {
			found = true
			break
		}
	}

	if !found {
		err = e.CreateDatabase(defaultDB, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "maho: %s: %s\n", defaultDB, err)
			return
		}
	}

	for idx, arg := range sqlArgs {
		svr.Handle(strings.NewReader(arg), os.Stdout, "startup", "sql-arg", fmt.Sprintf("%d", idx),
			false)
	}

	args := flag.Args()
	for idx := 0; idx < len(args); idx++ {
		f, err := os.Open(args[idx])
		if err != nil {
			fmt.Fprintf(os.Stderr, "maho: sql file: %s\n", err)
			return
		}
		svr.Handle(bufio.NewReader(f), os.Stderr, "startup", "sql-file", args[idx], false)
	}

	if *sshServer {
		sshCfg := server.SSHConfig{
			Address: *sshPort,
		}

		if len(hostKeys) == 0 {
			hostKeys = []string{"id_rsa"}
		}

		for _, hostKey := range hostKeys {
			keyBytes, err := ioutil.ReadFile(hostKey)
			if err != nil {
				fmt.Fprintf(os.Stderr, "maho: host keys: %s\n", err)
				return
			}
			sshCfg.HostKeysBytes = append(sshCfg.HostKeysBytes, keyBytes)
		}

		if *authorizedKeys != "" {
			sshCfg.AuthorizedBytes, err = ioutil.ReadFile(*authorizedKeys)
			if err != nil {
				fmt.Fprintf(os.Stderr, "maho: authorized keys: %s\n", err)
				return
			}
		}

		if len(userPasswords) > 0 {
			sshCfg.CheckPassword = func(user, password string) error {
				pw, ok := userPasswords[user]
				if !ok {
					return fmt.Errorf("user %s not found", user)
				}
				if password != pw {
					return fmt.Errorf("bad password for user %s", user)
				}
				return nil
			}
		}

		go func() {
			fmt.Fprintf(os.Stderr, "maho: %s\n", svr.ListenAndServeSSH(sshCfg))
		}()
	}

	if *repl || (!*sshServer && len(args) == 0 && len(sqlArgs) == 0) {
		svr.Handle(bufio.NewReader(os.Stdin), os.Stdout, "startup", "console", "", true)
	} else if *sshServer {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt)

		fmt.Println("maho: waiting for ^C to shutdown")
		<-ch
		go func() {
			<-ch
			os.Exit(0)
		}()
	}

	if *sshServer {
		fmt.Println("maho: shutting down")
		svr.Shutdown(context.Background())
	}
	log.WithField("pid", os.Getpid()).Info("maho done")
}
