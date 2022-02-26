package test_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/leftmike/sqltest/sqltestdb"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/flags"
	"github.com/leftmike/maho/server"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage/kvrows"
)

func TestProto3(t *testing.T) {
	cleanDir(t)

	dataDir := filepath.Join("testdata", "proto3")
	os.MkdirAll(dataDir, 0755)

	st, err := kvrows.NewPebbleStore(dataDir, log.StandardLogger())
	if err != nil {
		t.Fatal(err)
	}

	e := engine.NewEngine(st, flags.Default())
	e.CreateDatabase(sql.ID("test"), nil)

	svr := server.Server{
		Engine:          e,
		DefaultDatabase: sql.ID("test"),
	}

	go func() {
		svr.ListenAndServeProto3(server.Proto3Config{Address: "localhost:35432"})
	}()

	var run sqltestdb.DBRunner
	var retries int
	for {
		err = run.Connect("postgres", "host=localhost port=35432 dbname=test sslmode=disable")
		if err == nil {
			break
		}
		retries += 1
		if retries > 3 {
			t.Fatal(err)
		}
		time.Sleep(time.Second * time.Duration(retries))
	}

	testSQL(t, "proto3", &run, *sqltestData, false, false)
	err = run.Close()
	if err != nil {
		t.Fatal(err)
	}

	svr.Shutdown(context.Background())
}
