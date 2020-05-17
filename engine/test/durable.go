package test

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

var (
	durableTests = []struct {
		dbname sql.Identifier
		tests  []interface{}
	}{
		{sql.ID("durable_database_test"), databaseTests},
		{sql.ID("durable_tbl_lifecycle_test"), tableLifecycleTests},
		{sql.ID("durable_table_test"), tableTests},
		{sql.ID("durable_schema_test"), schemaTests},
		{sql.ID("durable_table_rows_test"), tableRowsTests},
	}
)

func DurableTableLifecycleTest(t *testing.T) {
	for grp := range durableTests {
		for num := range durableTests[grp].tests {
			cmd := exec.Command(os.Args[0], "-test.run=TestDurableHelper")
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

func DurableHelper(t *testing.T, createEng func() (engine.Engine, error)) {
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

	e, err := createEng()
	if err != nil {
		t.Fatal(err)
	}

	runTest(t, e, durableTests[grp].dbname, durableTests[grp].tests[num])
}
