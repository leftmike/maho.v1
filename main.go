package main

/*
To Do:
- fuzzing: parser.Parse

- add test for not seeing modified rows within a single SQL statement

- add type sql.ColumnValue interface{} and type BoolColumn []bool, type Int64Column []int64, etc
- Rows.NextColumns(ctx context.Context, destCols []sql.ColumnValue) error

- specify a subset of columns to return: Table.Rows(cols []int, ...)

- use olekukonko/tablewriter for table output

- use cockroachdb/pebble as a storage engine with kvrows

- use spf13/cobra for command argument handling

- use etcd-io/etcd/raft

- make sure all Rows get properly closed

- get rid of engine.ListDatabases: only used at startup in main.go

- storage/service might no longer be necessary?

- add protobuf column type

- select: ORDER BY: column(s) can have [table '.']

- tests with 1000s to 100000s of rows
-- generate rows
-- use sample databases

- kvrows
-- cleanup proposals
-- consider making Rows() incremental, maybe as blocks of rows
-- badger: usda.sql: can't load largest table; can't select from tables

- rowcols
-- snapshot store and truncate WAL
-- usda.sql: causes corrupt WAL

- proto3 (postgres protocol)
-- use binary format for oid.T_bool, T_bytea, T_float4, T_float8, T_int2, T_int4, T_int8

- test subquery expressions: '(' select | values | show ')'

- subquery expressions: EXISTS, IN, NOT IN, ANY/SOME, ALL
- conditional expressions: CASE, COALESCE, NULLIF, GREATEST, LEAST

- EXPLAIN
-- group by fields: need to get name of compiled aggregator
-- include full column names
-- SELECT: track where columns come from, maybe as part of Plan
-- DELETE, INSERT, UPDATE, VALUES

- ALTER TABLE ONLY table ADD CONSTRAINT constraint FOREIGN KEY ...

- foreign key references
-- need read lock on referenced keys

- references from foreign keys (ForeignRef?)
-- use index on foreign key table if available

- constraints
-- DROP TABLE: CASCADE: to remove foreign key constraint of another table
-- drop CHECK constraint: use ALTER TABLE table DROP CONSTRAINT constraint
-- drop DEFAULT: use ALTER TABLE table ALTER COLUMN column DROP DEFAULT (or DROP CONSTRAINT)
-- drop FOREIGN KEY: use ALTER TABLE table DROP CONSTRAINT constraint
-- drop NOT NULL: use ALTER TABLE table ALTER COLUMN column DROP NOT NULL (or DROP CONSTRAINT)
-- drop UNIQUE: use DROP INDEX ...
*/

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/flags"
	"github.com/leftmike/maho/repl"
	"github.com/leftmike/maho/server"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/basic"
	"github.com/leftmike/maho/storage/keyval"
	"github.com/leftmike/maho/storage/kvrows"
	"github.com/leftmike/maho/storage/rowcols"
)

var (
	cfg      = config.NewConfig(flag.CommandLine)
	database = cfg.Var(new(string), "database").Usage("default `database` (maho)").String("maho")
	store    = cfg.Var(new(string), "store").Usage("`store` (basic)").String("basic")
	dataDir  = cfg.Var(new(string), "data").
			Usage("`directory` containing databases (testdata)").String("testdata")
	proto3Server = cfg.Var(new(bool), "proto3").
			Usage("`flag` to control serving PostgreSQL wire protocol v3 (true)").Bool(true)
	proto3Port = cfg.Var(new(string), "port").
			Usage("`port` used to serve PostgreSQL wire protocol v3 (localhost:5432)").
			String("localhost:5432")
	sshServer = cfg.Var(new(bool), "ssh").
			Usage("`flag` to control serving SSH (false)").Bool(false)
	sshPort = cfg.Var(new(string), "ssh-port").
		Usage("`port` used to serve SSH (localhost:8241)").String("localhost:8241")
	logFile = cfg.Var(new(string), "log-file").Usage("`file` to use for logging (maho.log)").
		String("maho.log")
	logLevel = cfg.Var(new(string), "log-level").
			Usage("log level: debug, info, warn, error, fatal, or panic (info)").String("info")
	authorizedKeys = cfg.Var(new(string), "ssh-authorized-keys").
			Usage("`file` containing authorized ssh keys").String("")
	accounts   = cfg.Var(new(config.Array), "accounts").Hide().Array()
	configFile = flag.String("config-file", "", "`file` to load config from (maho.hcl)")
	noConfig   = flag.Bool("no-config", false, "don't load config file")
	listConfig = flag.Bool("list-config", false, "list config and exit")
	replFlag   = flag.Bool("repl", false, "`flag` to control the console repl (false)")
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

func makeConfigTable(tn sql.TableName, cfg *config.Config) (sql.Table, sql.TableType, error) {
	values := [][]sql.Value{}

	for _, v := range cfg.Vars() {
		values = append(values,
			[]sql.Value{
				sql.StringValue(v.Name()),
				sql.StringValue(v.By()),
				sql.BoolValue(v.Hidden()),
				sql.StringValue(v.Val()),
			})
	}

	return engine.MakeVirtualTable(tn,
		[]sql.Identifier{sql.ID("name"), sql.ID("by"), sql.ID("hidden"), sql.ID("value")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.BoolColType, sql.StringColType}, values)
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

	flgs := flags.Config(cfg)
	flag.Parse()
	cfg.Env()

	if *noConfig == false {
		filename := "maho.hcl"
		if *configFile != "" {
			filename = *configFile
		}
		err := cfg.Load(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "maho: %s: %s\n", *configFile, err)
			return
		}
	}
	if *listConfig {
		for _, v := range cfg.Vars() {
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
	case "kvrows":
		st, err = kvrows.NewBadgerStore(*dataDir)
	default:
		fmt.Fprintf(os.Stderr,
			"maho: got %s for store; want basic, rowcols, badger, bbolt, or kvrows", *store)
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "maho: %s", err)
		return
	}

	e := engine.NewEngine(st, flgs)
	e.CreateSystemInfoTable(sql.CONFIG,
		func(ctx context.Context, tx sql.Transaction, tn sql.TableName) (sql.Table, sql.TableType,
			error) {

			return makeConfigTable(tn, cfg)
		})

	svr := server.Server{
		Engine:          e,
		DefaultDatabase: sql.ID(*database),
	}

	tx := e.Begin(0)
	dbs, err := tx.ListDatabases(context.Background())
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
		svr.HandleSession(repl.Handler(strings.NewReader(arg), os.Stdout), "startup", "sql-arg",
			fmt.Sprintf("%d", idx))
	}

	args := flag.Args()
	for idx := 0; idx < len(args); idx++ {
		f, err := os.Open(args[idx])
		if err != nil {
			fmt.Fprintf(os.Stderr, "maho: sql file: %s\n", err)
			return
		}
		svr.HandleSession(repl.Handler(bufio.NewReader(f), os.Stderr), "startup", "sql-file",
			args[idx])
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

	if *proto3Server {
		p3Cfg := server.Proto3Config{
			Address: *proto3Port,
		}

		go func() {
			fmt.Fprintf(os.Stderr, "maho: %s\n", svr.ListenAndServeProto3(p3Cfg))
		}()
	}

	if *replFlag || (!*sshServer && !*proto3Server && len(args) == 0 && len(sqlArgs) == 0) {
		svr.HandleSession(repl.Interact(), "startup", "console", "")
	} else if *sshServer || *proto3Server {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt)

		fmt.Println("maho: waiting for ^C to shutdown")
		<-ch
		go func() {
			<-ch
			os.Exit(0)
		}()
	}

	if *sshServer || *proto3Server {
		fmt.Println("maho: shutting down")
		svr.Shutdown(context.Background())
	}
	log.WithField("pid", os.Getpid()).Info("maho done")
}
