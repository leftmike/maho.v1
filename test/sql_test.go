package test_test

import (
	"flag"
	"fmt"
	"testing"

	"github.com/leftmike/sqltest/pkg/sqltest"

	"github.com/leftmike/maho/engine"
	_ "github.com/leftmike/maho/engine/basic"
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
	testData = flag.String("testdata", "../../sqltest/sql/testdata", "directory of testdata")
)

type mahoDialect struct {
	sqltest.DefaultDialect
}

func (_ mahoDialect) DriverName() string {
	return "maho"
}

var started bool

func startEngine(t *testing.T) {
	t.Helper()

	if !started {
		err := engine.Start("basic", "testdata", sql.ID("test"))
		if err != nil {
			t.Fatal(err)
		}
		started = true
	}
}

func TestSQL(t *testing.T) {
	startEngine(t)

	run := test.Runner{}
	var rptr reporter
	err := sqltest.RunTests(*testData, &run, &rptr, mahoDialect{}, *update)
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
