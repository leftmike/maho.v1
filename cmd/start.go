package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/leftmike/maho/engine"
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
	startCmd = &cobra.Command{
		Use:   "start",
		Short: "Start the Maho database server",
		RunE:  startRun,
	}

	database = "maho"
	store    = "basic"
	dataDir  = "testdata"

	proto3Host     = "localhost"
	proto3Port     = "5432"
	sshServer      = false
	sshPort        = "localhost:8241"
	authorizedKeys = ""
	hostKeys       = []string{"id_rsa"}

	sqlArgs = []string{}
)

func initServerFlags(fs *pflag.FlagSet) {
	fs.StringVar(&database, "database", database, "default `database`")
	cfgVars["database"] = fs.Lookup("database")

	fs.StringVar(&store, "store", store, "storage engine to use")
	cfgVars["store"] = fs.Lookup("store")

	fs.StringVar(&dataDir, "data", dataDir, "`directory` containing databases")
	cfgVars["data"] = fs.Lookup("data")

	fs.StringSliceVar(&sqlArgs, "sql", sqlArgs, "sql `query` to execute; multiple allowed")
}

func init() {
	fs := startCmd.Flags()
	initServerFlags(fs)

	fs.StringVar(&proto3Host, "host", proto3Host,
		"`host` used to serve PostgreSQL wire protocol v3")
	cfgVars["host"] = fs.Lookup("host")

	fs.StringVarP(&proto3Port, "port", "p", proto3Port,
		"`port` used to serve PostgreSQL wire protocol v3")
	cfgVars["port"] = fs.Lookup("port")

	fs.BoolVar(&sshServer, "ssh", sshServer, "`flag` to control serving SSH")
	cfgVars["ssh"] = fs.Lookup("ssh")

	fs.StringVar(&sshPort, "ssh-port", sshPort, "`port` used to serve SSH")
	cfgVars["ssh-port"] = fs.Lookup("ssh-port")

	fs.StringVar(&authorizedKeys, "ssh-authorized-keys", authorizedKeys,
		"`file` containing authorized ssh keys")
	cfgVars["ssh-authorized-keys"] = fs.Lookup("ssh-authorized-keys")

	fs.StringSliceVar(&hostKeys, "ssh-host-key", hostKeys,
		"`file` containing a ssh host key; multiple allowed")
	cfgVars["ssh-host-keys"] = fs.Lookup("ssh-host-key")

	cfgVars["accounts"] = nil

	mahoCmd.AddCommand(startCmd)
}

func makeConfigTable(tn sql.TableName) (sql.Table, sql.TableType, error) {
	values := [][]sql.Value{}

	for name, flg := range cfgVars {
		var used bool
		if flg != nil {
			_, ok := usedFlags[flg.Name]
			if ok {
				used = true
			}
		}

		var val string
		var by string

		if used {
			val = flg.Value.String()
			by = "flag"
		} else if obj, ok := cfg[name]; ok {
			if _, ok := obj.([]interface{}); ok {
				val = "..."
			} else if _, ok := obj.(map[string]interface{}); ok {
				val = "..."
			} else {
				val = fmt.Sprintf("%v", obj)
			}
			by = "config"
		} else if flg != nil {
			val = flg.DefValue
			by = "default"
		} else {
			continue
		}

		values = append(values,
			[]sql.Value{
				sql.StringValue(name),
				sql.StringValue(by),
				sql.StringValue(val),
			})
	}

	return engine.MakeVirtualTable(tn,
		[]sql.Identifier{sql.ID("name"), sql.ID("by"), sql.ID("value")},
		[]sql.ColumnType{sql.IdColType, sql.IdColType, sql.StringColType}, values)
}

func newServer(args []string) (*server.Server, error) {
	var st *storage.Store
	var err error
	switch store {
	case "basic":
		st, err = basic.NewStore(dataDir)
	case "rowcols":
		st, err = rowcols.NewStore(dataDir)
	case "badger":
		st, err = keyval.NewBadgerStore(dataDir, log.StandardLogger())
	case "bbolt":
		st, err = keyval.NewBBoltStore(dataDir)
	case "kvrows":
		st, err = kvrows.NewBadgerStore(dataDir, log.StandardLogger())
	case "pebble":
		st, err = kvrows.NewPebbleStore(dataDir, log.StandardLogger())
	default:
		return nil,
			fmt.Errorf(
				"maho: got %s for store; want basic, rowcols, badger, bbolt, kvrows, or pebble",
				store)
	}
	if err != nil {
		return nil, fmt.Errorf("maho: %s", err)
	}

	e := engine.NewEngine(st, flgs)
	e.CreateSystemInfoTable(sql.CONFIG,
		func(ctx context.Context, tx sql.Transaction, tn sql.TableName) (sql.Table, sql.TableType,
			error) {

			return makeConfigTable(tn)
		})

	defaultDB := sql.ID(database)
	valid, err := e.ValidDatabase(defaultDB)
	if err != nil {
		return nil, fmt.Errorf("maho: valid database: %s", err)
	}
	if !valid {
		err = e.CreateDatabase(defaultDB, nil)
		if err != nil {
			return nil, fmt.Errorf("maho: %s: %s", defaultDB, err)
		}
	}

	svr := &server.Server{
		Engine:          e,
		DefaultDatabase: defaultDB,
	}

	for idx, arg := range sqlArgs {
		svr.HandleSession(repl.Handler(strings.NewReader(arg), os.Stdout), "startup", "sql-arg",
			strconv.Itoa(idx))
	}

	for idx := 0; idx < len(args); idx++ {
		f, err := os.Open(args[idx])
		if err != nil {
			return nil, fmt.Errorf("maho: sql file: %s", err)
		}
		svr.HandleSession(repl.Handler(bufio.NewReader(f), os.Stderr), "startup", "sql-file",
			args[idx])
		f.Close()
	}

	return svr, nil
}

func userAccounts() map[string]string {
	val := cfg["accounts"]
	if val == nil {
		return nil
	}
	slice, ok := val.([]interface{})
	if !ok {
		return nil
	}

	userPasswords := map[string]string{}
	for _, obj := range slice {
		account, ok := obj.(map[string]interface{})
		if !ok {
			return nil
		}
		user, ok := account["user"].(string)
		if !ok {
			return nil
		}
		password, ok := account["password"].(string)
		if !ok {
			return nil
		}
		userPasswords[user] = password
	}

	return userPasswords
}

func startRun(cmd *cobra.Command, args []string) error {
	svr, err := newServer(args)
	if err != nil {
		return err
	}

	p3Cfg := server.Proto3Config{
		Address: fmt.Sprintf("%s:%s", proto3Host, proto3Port),
	}

	go func() {
		fmt.Fprintf(os.Stderr, "maho: %s\n", svr.ListenAndServeProto3(p3Cfg))
	}()

	if sshServer {
		userPasswords := userAccounts()

		sshCfg := server.SSHConfig{
			Address: sshPort,
		}

		for _, hostKey := range hostKeys {
			keyBytes, err := ioutil.ReadFile(hostKey)
			if err != nil {
				return fmt.Errorf("maho: host keys: %s", err)
			}
			sshCfg.HostKeysBytes = append(sshCfg.HostKeysBytes, keyBytes)
		}

		if authorizedKeys != "" {
			sshCfg.AuthorizedBytes, err = ioutil.ReadFile(authorizedKeys)
			if err != nil {
				return fmt.Errorf("maho: authorized keys: %s", err)
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

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	fmt.Println("maho: waiting for ^C to shutdown")
	<-ch
	go func() {
		<-ch
		os.Exit(0)
	}()

	fmt.Println("maho: shutting down")
	svr.Shutdown(context.Background())

	return nil
}
