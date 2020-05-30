package test_test

import (
	"flag"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/leftmike/sqltest/pkg/sqltest"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/engine/basic"
	"github.com/leftmike/maho/engine/keyval"
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
	name string
}

func (md mahoDialect) DriverName() string {
	return md.name
}

func testSQL(t *testing.T, typ string, dbname sql.Identifier, testData, dataDir string) {
	t.Helper()

	var e engine.Engine
	var err error
	switch typ {
	case "basic":
		e, err = basic.NewEngine(dataDir)
	case "memrows":
		e, err = memrows.NewEngine(dataDir)
	case "rowcols":
		e, err = rowcols.NewEngine(dataDir)
	case "keyval":
		e, err = keyval.NewEngine(dataDir)
	default:
		panic(fmt.Sprintf("unexpected engine type: %s", typ))
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
	err = sqltest.RunTests(testData, &run, &rptr, mahoDialect{name: "maho-" + typ}, *update)
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
	testSQL(t, "basic", sql.ID("test_basic"), "testdata", "")
	testSQL(t, "basic", sql.ID("sqltest_basic"), *testData, "")
}

func TestSQLMemRows(t *testing.T) {
	testSQL(t, "memrows", sql.ID("test_memrows"), "testdata", "")
	testSQL(t, "memrows", sql.ID("sqltest_memrows"), *testData, "")
}

func TestSQLRowCols(t *testing.T) {
	dataDir := filepath.Join("testdata", "rowcols")

	err := testutil.CleanDir(dataDir, []string{".gitignore", "expected", "output", "sql"})
	if err != nil {
		t.Fatal(err)
	}
	testSQL(t, "rowcols", sql.ID("test_rowcols"), "testdata", dataDir)

	err = testutil.CleanDir(dataDir, []string{".gitignore", "expected", "output", "sql"})
	if err != nil {
		t.Fatal(err)
	}
	testSQL(t, "rowcols", sql.ID("sqltest_rowcols"), *testData, dataDir)
}

func TestSQLKeyVal(t *testing.T) {
	dataDir := filepath.Join("testdata", "keyval")

	err := testutil.CleanDir(dataDir, []string{".gitignore", "expected", "output", "sql"})
	if err != nil {
		t.Fatal(err)
	}
	testSQL(t, "keyval", sql.ID("test_keyval"), "testdata", dataDir)

	err = testutil.CleanDir(dataDir, []string{".gitignore", "expected", "output", "sql"})
	if err != nil {
		t.Fatal(err)
	}
	testSQL(t, "keyval", sql.ID("sqltest_keyval"), *testData, dataDir)
}
