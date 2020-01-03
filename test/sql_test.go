package test_test

import (
	"flag"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/leftmike/sqltest/pkg/sqltest"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/engine/memrows"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/test"
)

type report struct {
	test string
	err  error
}

type reporter []report

func (r *reporter) Report(test string, err error) error {
	if err == nil {
		fmt.Printf("%s: passed\n", test)
	} else if err == sqltest.Skipped {
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
		filepath.Join("..", "..", "sqltest", "sql", "testdata"), "directory of testdata")
)

type mahoDialect struct {
	sqltest.DefaultDialect
}

func (_ mahoDialect) DriverName() string {
	return "maho"
}

func testSQL(t *testing.T, typ string, dbname sql.Identifier, testData string) {
	t.Helper()

	var e engine.Engine
	var err error
	switch typ {
	case "basic":
		e, err = basic.NewEngine("testdata")
	case "memrows":
		e, err = memrows.NewEngine("testdata")
	}
	if err != nil {
		t.Fatal(err)
	}
	err = e.CreateDatabase(dbname, nil)
	if err != nil {
		// If the test is run multiple times, then the database will already exist.
	}

	run := test.Runner{
		Engine:   e,
		Database: dbname,
	}
	var rptr reporter
	err = sqltest.RunTests(testData, &run, &rptr, mahoDialect{}, *update)
	if err != nil {
		t.Errorf("RunTests(%q) failed with %s", testData, err)
		return
	}
	for _, report := range rptr {
		if report.err != nil && report.err != sqltest.Skipped {
			t.Errorf("%s: %s", report.test, report.err)
		}
	}
}

func TestSQLBasic(t *testing.T) {
	testSQL(t, "basic", sql.ID("test_basic"), "testdata")
	testSQL(t, "basic", sql.ID("sqltest_basic"), *testData)
}

func TestSQLMemRows(t *testing.T) {
	testSQL(t, "memrows", sql.ID("test_memrows"), "testdata")
	testSQL(t, "memrows", sql.ID("sqltest_memrows"), *testData)
}
