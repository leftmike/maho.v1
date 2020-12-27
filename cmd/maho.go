package cmd

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/hashicorp/hcl"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/leftmike/maho/flags"
)

var (
	mahoCmd = &cobra.Command{
		Use:               "maho",
		Short:             "A database server",
		Long:              "Maho is a PostgreSQL compatible database server.",
		PersistentPreRunE: mahoPreRun,
		PersistentPostRun: mahoPostRun,
	}

	logFile   = "maho.log"
	logLevel  = "info"
	logStderr = false
	logWriter io.WriteCloser

	configFile = "maho.hcl"
	noConfig   = false

	cfgVars   = map[string]*pflag.Flag{}
	cfg       = map[string]interface{}{}
	flgs      = flags.Default()
	usedFlags = map[string]struct{}{}
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableLevelTruncation: true,
	})

	fs := mahoCmd.PersistentFlags()

	fs.StringVar(&logFile, "log-file", logFile, "`file` to use for logging")
	cfgVars["log-file"] = fs.Lookup("log-file")

	fs.StringVar(&logLevel, "log-level", logLevel,
		"log level: trace, debug, info, warn, error, fatal, or panic")
	cfgVars["log-level"] = fs.Lookup("log-level")

	fs.BoolVarP(&logStderr, "log-stderr", "s", logStderr, "log to standard error")

	fs.StringVar(&configFile, "config-file", configFile, "`file` to load config from")
	fs.BoolVar(&noConfig, "no-config", noConfig, "don't load config file")
}

func Execute() error {
	return mahoCmd.Execute()
}

func mahoPreRun(cmd *cobra.Command, args []string) error {
	cmd.Flags().Visit(
		func(flg *pflag.Flag) {
			usedFlags[flg.Name] = struct{}{}
		})

	if configFile != "" && !noConfig {
		err := loadConfig()
		if err != nil {
			return fmt.Errorf("maho: %s", err)
		}
	}

	if !logStderr && logFile != "" {
		var err error
		logWriter, err = os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			logWriter = nil
			return fmt.Errorf("maho: %s", err)
		}
		log.SetOutput(logWriter)
	}

	ll, err := log.ParseLevel(logLevel)
	if err != nil {
		return fmt.Errorf("maho: %s", err)
	}
	log.SetLevel(ll)

	log.WithField("pid", os.Getpid()).Info("maho starting")
	return nil
}

func mahoPostRun(cmd *cobra.Command, args []string) {
	log.WithField("pid", os.Getpid()).Info("maho done")

	if logWriter != nil {
		logWriter.Close()
	}
}

func loadConfig() error {
	b, err := ioutil.ReadFile(configFile)
	if err != nil {
		return err
	}

	err = hcl.Decode(&cfg, string(b))
	if err != nil {
		return err
	}

	for name, val := range cfg {
		if flg, ok := cfgVars[name]; ok {
			if flg == nil {
				continue
			}
			if _, ok := usedFlags[flg.Name]; ok {
				continue
			}
			err := flg.Value.Set(fmt.Sprintf("%v", val))
			if err != nil {
				return fmt.Errorf("%s: %s", name, err)
			}
		} else if f, ok := flags.LookupFlag(name); ok {
			b, ok := val.(bool)
			if !ok {
				return fmt.Errorf("%s: expected boolean value; got %v", name, val)
			}
			flgs[f] = b
		} else {
			return fmt.Errorf("%s is not a config variable", name)
		}
	}

	return nil
}
