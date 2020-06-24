package test_test

import (
	"flag"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/leftmike/sqltest/sqltestdb"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/engine/keyval"
	"github.com/leftmike/maho/engine/kvrows"
	"github.com/leftmike/maho/engine/memrows"
	"github.com/leftmike/maho/engine/rowcols"
	"github.com/leftmike/maho/sql"
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
	update   = flag.Bool("update", false, "update expected to output")
	testData = flag.String("testdata",
		filepath.Join("..", "..", "sqltest", "testdata"), "directory of testdata")
)

type mahoDialect struct {
	sqltestdb.DefaultDialect
	name string
}

func (md mahoDialect) DriverName() string {
	return md.name
}

func testSQL(t *testing.T, typ string, e engine.Engine, testData string, psql bool) {
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

func testAllSQL(t *testing.T, typ string, clean bool, makeEng func() engine.Engine) {
	if clean {
		err := testutil.CleanDir("testdata", []string{".gitignore", "expected", "output", "sql"})
		if err != nil {
			t.Fatal(err)
		}
	}
	testSQL(t, typ, makeEng(), "testdata", false)

	if clean {
		err := testutil.CleanDir("testdata", []string{".gitignore", "expected", "output", "sql"})
		if err != nil {
			t.Fatal(err)
		}
	}
	testSQL(t, typ, makeEng(), *testData, false)

	if clean {
		err := testutil.CleanDir("testdata", []string{".gitignore", "expected", "output", "sql"})
		if err != nil {
			t.Fatal(err)
		}
	}
	testSQL(t, typ, makeEng(), filepath.Join(*testData, "postgres"), true)

	//	if typ == "basic" || typ == "memrows" || typ == "rowcols" || typ == "badger" ||
	//		typ == "bbolt" || typ == "kvrows" {

	if clean {
		err := testutil.CleanDir("testdata", []string{".gitignore", "expected", "output", "sql"})
		if err != nil {
			t.Fatal(err)
		}
	}
	testSQL(t, typ, makeEng(), "../../postgres-tests", true)
	//	}
}

func TestSQLBasic(t *testing.T) {
	testAllSQL(t, "basic", false, func() engine.Engine {
		e, err := basic.NewEngine("")
		if err != nil {
			t.Fatal(err)
		}
		return e
	})
}

func TestSQLMemRows(t *testing.T) {
	testAllSQL(t, "memrows", false,
		func() engine.Engine {
			e, err := memrows.NewEngine("")
			if err != nil {
				t.Fatal(err)
			}
			return e
		})
}

func TestSQLRowCols(t *testing.T) {
	testAllSQL(t, "rowcols", true,
		func() engine.Engine {
			e, err := rowcols.NewEngine(filepath.Join("testdata", "rowcols"))
			if err != nil {
				t.Fatal(err)
			}
			return e
		})
}

func TestSQLBadger(t *testing.T) {
	testAllSQL(t, "badger", true,
		func() engine.Engine {
			e, err := keyval.NewBadgerEngine(filepath.Join("testdata", "badger"))
			if err != nil {
				t.Fatal(err)
			}
			return e
		})
}

func TestSQLBBolt(t *testing.T) {
	testAllSQL(t, "bbolt", true,
		func() engine.Engine {
			e, err := keyval.NewBBoltEngine("testdata")
			if err != nil {
				t.Fatal(err)
			}
			return e
		})
}

func TestSQLKVRows(t *testing.T) {
	testAllSQL(t, "kvrows", true,
		func() engine.Engine {
			e, err := kvrows.NewBadgerEngine(filepath.Join("testdata", "kvrows"))
			if err != nil {
				t.Fatal(err)
			}
			return e
		})
}
