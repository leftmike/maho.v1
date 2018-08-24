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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/leftmike/maho/config"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/engine/memrows"
	"github.com/leftmike/maho/server"
	"github.com/leftmike/maho/sql"
)

const (
	prompt = "maho> "
)

var (
	database = config.Var(new(string), "database").Usage("default `database` (maho)").String("maho")
	eng      = config.Var(new(string), "engine").Usage("default `engine` (basic)").String("basic")
	dataDir  = config.Var(new(string), "data-directory").
			Flag("data", "`directory` containing databases (./testdata)").String("testdata")
	sshServer = config.Var(new(bool), "ssh").
			Usage("`flag` to control serving ssh (true)").Bool(true)
	sshPort = config.Var(new(string), "ssh-port").
		Usage("`port` used to serve ssh (localhost:8241)").String("localhost:8241")
	logFile = config.Var(new(string), "log-file").Usage("`file` to use for logging (./maho.log)").
		String("maho.log")
	logLevel = config.Var(new(string), "log-level").
			Usage("log level: debug, info, warn, error, fatal, or panic (info)").String("info")
	authorizedKeys = config.Var(new(string), "ssh-authorized-keys").
			Usage("`file` containing authorized ssh keys").String("")

	accounts = config.Var(new(config.Array), "accounts").Array()

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

func parseAccounts(accounts config.Array) (map[string]string, bool) {
	userPasswords := map[string]string{}
	for _, a := range accounts {
		account, ok := a.(config.Map)
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
		filename := filepath.Join(".", "maho.cfg")
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

	mgr := engine.NewManager(*dataDir, map[string]engine.Engine{
		"basic":   basic.Engine{},
		"memrows": memrows.Engine{},
	})

	err := mgr.CreateDatabase(*eng, sql.ID(*database), engine.Options{sql.WAIT: "true"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "maho: %s: %s\n", *database, err)
		return
	}

	for idx, arg := range sqlArgs {
		replSQL(mgr, strings.NewReader(arg), fmt.Sprintf("sql arg %d", idx+1), os.Stdout, "")
	}

	args := flag.Args()
	for idx := 0; idx < len(args); idx++ {
		f, err := os.Open(args[idx])
		if err != nil {
			fmt.Fprintf(os.Stderr, "maho: sql file: %s\n", err)
			return
		}
		replSQL(mgr, bufio.NewReader(f), args[idx], os.Stderr, "")
	}

	if *sshServer {
		if len(hostKeys) == 0 {
			hostKeys = []string{"id_rsa"}
		}

		var hostKeysBytes [][]byte
		for _, hostKey := range hostKeys {
			keyBytes, err := ioutil.ReadFile(hostKey)
			if err != nil {
				fmt.Fprintf(os.Stderr, "maho: host keys: %s\n", err)
				return
			}
			hostKeysBytes = append(hostKeysBytes, keyBytes)
		}

		var authorizedBytes []byte
		if *authorizedKeys != "" {
			authorizedBytes, err = ioutil.ReadFile(*authorizedKeys)
			if err != nil {
				fmt.Fprintf(os.Stderr, "maho: authorized keys: %s\n", err)
				return
			}
		}

		var checkPassword func(user, password string) error
		if len(userPasswords) > 0 {
			checkPassword = func(user, password string) error {
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

		ss, err := server.NewSSHServer(*sshPort, hostKeysBytes, prompt, authorizedBytes,
			checkPassword)
		if err != nil {
			fmt.Fprintf(os.Stderr, "maho: ssh server: %s\n", err)
			return
		}
		serve := func(c *server.Client) {
			replSQL(mgr, c.RuneReader, fmt.Sprintf("%s@%s:%s", c.User, c.Type, c.Addr), c.Writer,
				"")
		}
		if *repl {
			go func() {
				fmt.Fprintf(os.Stderr, "maho: ssh server: %s\n",
					ss.ListenAndServe(server.HandlerFunc(serve)))
			}()
		} else {
			fmt.Fprintf(os.Stderr, "maho: ssh server: %s\n",
				ss.ListenAndServe(server.HandlerFunc(serve)))
		}
	}

	if *repl || (!*sshServer && len(args) == 0 && len(sqlArgs) == 0) {
		replSQL(mgr, bufio.NewReader(os.Stdin), "<console>", os.Stdout, prompt)
	}

	log.WithField("pid", os.Getpid()).Info("maho done")
}
