package test

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

var (
	durableTests = []struct {
		dbname sql.Identifier
		tests  []interface{}
		short  bool
	}{
		{sql.ID("durable_database_test"), databaseTests, false},
		{sql.ID("durable_tbl_lifecycle_test"), tableLifecycleTests, true},
		{sql.ID("durable_table_test"), tableTests, false},
		{sql.ID("durable_schema_test"), schemaTests, true},
		{sql.ID("durable_table_rows_test"), tableRowsTests, false},
	}
)

func DurableTests(t *testing.T, short bool, helper string) {
	for grp := range durableTests {
		if durableTests[grp].short && short {
			continue
		}

		for num := range durableTests[grp].tests {
			cmd := exec.Command(os.Args[0], fmt.Sprintf("-test.run=%s", helper))
			cmd.Env = append(
				append(os.Environ(), fmt.Sprintf("MAHO_DURABLE_TEST=%d", num)),
				fmt.Sprintf("MAHO_DURABLE_GROUP=%d", grp))
			out, err := cmd.CombinedOutput()
			if len(out) > 0 {
				fmt.Print(string(out))
			}
			if err != nil {
				t.Errorf("durable test failed: group %s test %d", durableTests[grp].dbname, num)
			}
		}
	}
}

func DurableHelper(t *testing.T, createStore func() (*storage.Store, error)) {
	test := os.Getenv("MAHO_DURABLE_TEST")
	if test == "" {
		return
	}
	group := os.Getenv("MAHO_DURABLE_GROUP")
	if group == "" {
		return
	}
	num, err := strconv.Atoi(test)
	if err != nil {
		t.Fatal(err)
	}
	grp, err := strconv.Atoi(group)
	if err != nil {
		t.Fatal(err)
	}
	if grp < 0 || grp >= len(durableTests) {
		t.Fatalf("durable helper: group out of range: %d", grp)
	}
	if num < 0 || num >= len(durableTests[grp].tests) {
		t.Fatalf("durable helper: test out of range: %d", num)
	}

	st, err := createStore()
	if err != nil {
		t.Fatal(err)
	}

	runTest(t, st, durableTests[grp].dbname, durableTests[grp].tests[num])
}
