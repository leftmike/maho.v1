package test_test

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/leftmike/sqltest/sqltestdb"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/flags"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/basic"
	"github.com/leftmike/maho/storage/kvrows"
	"github.com/leftmike/maho/test"
	"github.com/leftmike/maho/testutil"
)

type report struct {
	test string
	err  error
}

type reporter []report

func (r *reporter) Report(test string, err error) error {
	if err == nil {
		fmt.Printf("%s: passed\n", test)
	} else if err == sqltestdb.Skipped {
		fmt.Printf("%s: skipped\n", test)
	} else {
		fmt.Printf("%s: failed: %s\n", test, err)
	}

	*r = append(*r, report{test, err})
	return nil
}

var (
	update      = flag.Bool("update", false, "update expected to output")
	sqltestData = flag.String("sqltest",
		filepath.Join("..", "..", "sqltest", "testdata"), "directory of sqltest data")
	postgresData = flag.String("postgres",
		filepath.Join("..", "..", "postgres-tests"), "directory of postgres test data")
)

type mahoDialect struct {
	sqltestdb.DefaultDialect
	name string
}

func (md mahoDialect) DriverName() string {
	return md.name
}

func testSQL(t *testing.T, typ string, run sqltestdb.Runner, testData string, update, psql bool) {
	var rptr reporter
	err := sqltestdb.RunTests(testData, run, &rptr, mahoDialect{name: "maho-" + typ}, update,
		psql)
	if err != nil {
		t.Errorf("RunTests(%q) failed with %s", testData, err)
		return
	}
	for _, report := range rptr {
		if report.err != nil && report.err != sqltestdb.Skipped {
			t.Errorf("%s: %s", report.test, report.err)
		}
	}
}

var (
	configs = []struct {
		name     string
		newStore func(dataDir string) (*storage.Store, error)
	}{
		{
			name: "badger",
			newStore: func(dataDir string) (*storage.Store, error) {
				return kvrows.NewBadgerStore(dataDir,
					testutil.SetupLogger(filepath.Join(dataDir, "badger_test.log")))
			},
		},
		{
			name:     "basic",
			newStore: basic.NewStore,
		},
		{
			name:     "bbolt",
			newStore: kvrows.NewBBoltStore,
		},
		{
			name: "btree",
			newStore: func(dataDir string) (*storage.Store, error) {
				return kvrows.NewBTreeStore()
			},
		},
		{
			name: "pebble",
			newStore: func(dataDir string) (*storage.Store, error) {
				return kvrows.NewPebbleStore(dataDir,
					testutil.SetupLogger(filepath.Join(dataDir, "pebble_test.log")))
			},
		},
	}

	cleaned bool
)

func cleanDir(t *testing.T) {
	if !cleaned {
		err := testutil.CleanDir("testdata", []string{".gitignore", "expected", "output", "sql"})
		if err != nil {
			t.Fatal(err)
		}
		cleaned = true
	}
}

func TestSQL(t *testing.T) {
	tests := []struct {
		name      string
		testData  string
		psql      bool
		skipShort bool
	}{
		{
			name:     "maho",
			testData: "testdata",
		},
		{
			name:     "sqltest",
			testData: *sqltestData,
		},
		{
			name:     "sqltest-postgres",
			testData: filepath.Join(*sqltestData, "postgres"),
			psql:     true,
		},
		{
			name:      "postgres",
			testData:  *postgresData,
			psql:      true,
			skipShort: true,
		},
	}

	cleanDir(t)

	for _, tst := range tests {
		if testing.Short() && tst.skipShort {
			continue
		}

		for _, cfg := range configs {
			t.Run(fmt.Sprintf("%s.%s", cfg.name, tst.name),
				func(t *testing.T) {
					dataDir := filepath.Join("testdata", tst.name, cfg.name)
					os.MkdirAll(dataDir, 0755)

					st, err := cfg.newStore(dataDir)
					if err != nil {
						t.Fatal(err)
					}

					e := engine.NewEngine(st, flags.Default())
					e.CreateDatabase(sql.ID("test"), nil)
					// Ignore errors: the database might already exist.

					run := ((*test.Runner)(evaluate.NewSession(e, sql.ID("test"), sql.PUBLIC)))
					testSQL(t, cfg.name, run, tst.testData, *update, tst.psql)
				})
		}
	}
}
