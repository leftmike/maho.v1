package testutil

import (
	"flag"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
)

var (
	logFile   = ""
	logLevel  = "info"
	logStderr = false
)

func init() {
	flag.StringVar(&logFile, "log-file", logFile, "`file` to use for logging")
	flag.StringVar(&logLevel, "log-level", logLevel,
		"log level: trace, debug, info, warn, error, fatal, or panic")
	flag.BoolVar(&logStderr, "log-stderr", logStderr, "log to standard error")
	flag.BoolVar(&logStderr, "s", logStderr, "log to standard error")
}

func SetupLogger(file string) *log.Logger {
	if !logStderr {
		if logFile != "" {
			file = logFile
		}

		w, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			panic(err)
		}
		fmt.Fprintln(w)
		log.SetOutput(w)
	}

	ll, err := log.ParseLevel(logLevel)
	if err != nil {
		panic(err)
	}
	log.SetLevel(ll)

	log.WithField("pid", os.Getpid()).Info("tests starting")
	return log.StandardLogger()
}
