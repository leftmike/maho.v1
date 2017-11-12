package test_test

import (
	"testing"

	"sqltest"

	"maho/test"
	"maho/testutil"
)

type report struct {
	test string
	err  error
}

type reporter []report

func (r *reporter) Report(test string, err error) error {
	*r = append(*r, report{test, err})
	return nil
}

var testData = "../../sqltest/testdata"

func TestSQL(t *testing.T) {
	e, _, err := testutil.StartEngine("test")
	if err != nil {
		t.Errorf("StartEngine() failed with %s", err)
		return
	}

	run := test.Runner{Engine: e}
	var reporter reporter
	err = sqltest.RunTests(testData, &run, &reporter, sqltest.Dialect{"maho"})
	if err != nil {
		t.Errorf("RunTests(%q) failed with %s", testData, err)
		return
	}
	for _, report := range reporter {
		if report.err != nil {
			t.Errorf("%s: %s", report.test, report.err)
		}
	}
}
