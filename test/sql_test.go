package test_test

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/leftmike/sqltest/sqltestdb"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/basic"
	"github.com/leftmike/maho/storage/keyval"
	"github.com/leftmike/maho/storage/kvrows"
	"github.com/leftmike/maho/storage/rowcols"
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

func testSQL(t *testing.T, typ string, e *engine.Engine, testData string, psql bool) {
	t.Helper()

	dbname := sql.ID("test")
	err := e.CreateDatabase(dbname, nil)
	if err != nil {
		// If the test is run multiple times, then the database will already exist.
	}

	run := test.Runner{
		Engine:   e,
		Database: dbname,
	}
	var rptr reporter
	err = sqltestdb.RunTests(testData, &run, &rptr, mahoDialect{name: "maho-" + typ}, *update,
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

func TestSQL(t *testing.T) {
	configs := []struct {
		name     string
		persist  bool
		newStore func(dataDir string) (storage.Store, error)
	}{
		{
			name:     "basic",
			newStore: basic.NewStore,
		},
		{
			name:     "rowcols",
			newStore: rowcols.NewStore,
		},
		{
			name:     "badger",
			newStore: keyval.NewBadgerStore,
		},
		{
			name:     "bbolt",
			newStore: keyval.NewBBoltStore,
		},
		{
			name:     "kvrows",
			newStore: kvrows.NewBadgerStore,
		},
	}

	tests := []struct {
		name     string
		testData string
		psql     bool
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
			name:     "postgres",
			testData: *postgresData,
			psql:     true,
		},
	}

	err := testutil.CleanDir("testdata", []string{".gitignore", "expected", "output", "sql"})
	if err != nil {
		t.Fatal(err)
	}

	for _, tst := range tests {
		for _, cfg := range configs {
			dataDir := filepath.Join("testdata", tst.name, cfg.name)
			os.MkdirAll(dataDir, 0755)

			st, err := cfg.newStore(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			e, err := engine.NewEngine(st)
			if err != nil {
				t.Fatal(err)
			}

			testSQL(t, cfg.name, e, tst.testData, tst.psql)
		}
	}
}
