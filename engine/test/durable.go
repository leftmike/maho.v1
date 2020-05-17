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
	durabilityDBName = sql.ID("durability_test")
	durabilityTests  = []interface{}{
		// TableLifecycleTest
		"cleanDir",
		[]dbCmd{
			{fln: fln(), cmd: cmdCreateDatabase, name: durabilityDBName},
		},
		[]engCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a"), fail: true},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]engCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-a"), fail: true},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]engCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-a"}},
			{fln: fln(), cmd: cmdCommit},
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-c")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-d")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-a", "tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]engCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-a", "tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]engCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]engCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]engCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-e")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d", "tbl-e"}},
			{fln: fln(), cmd: cmdRollback},
		},
		[]engCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]engCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl-c")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-d"}},
			{fln: fln(), cmd: cmdRollback},
		},
		[]engCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
	}
)

func RunDurabilityTest(t *testing.T, cleanDir func() error) {
	for testNum, dt := range durabilityTests {
		if s, ok := dt.(string); ok && s == "cleanDir" {
			err := cleanDir()
			if err != nil {
				t.Fatal(err)
			}
		} else {
			cmd := exec.Command(os.Args[0], "-test.run=TestDurabilityHelper")
			cmd.Env = append(os.Environ(), fmt.Sprintf("MAHO_DURABILITY_TEST=%d", testNum))
			out, err := cmd.CombinedOutput()
			if len(out) > 0 {
				fmt.Print(string(out))
			}
			if err != nil {
				t.Errorf("durability test block number %d failed", testNum)
			}
		}
	}
}

func DurabilityHelper(t *testing.T, createEng func() (engine.Engine, error)) {
	envVal := os.Getenv("MAHO_DURABILITY_TEST")
	if envVal == "" {
		return
	}
	testNum, err := strconv.Atoi(envVal)
	if err != nil {
		t.Fatal(err)
	}
	if testNum < 0 || testNum >= len(durabilityTests) {
		t.Fatalf("durability helper: test num out of range: %d", testNum)
	}

	e, err := createEng()
	if err != nil {
		t.Fatal(err)
	}

	switch tst := durabilityTests[testNum].(type) {
	case []engCmd:
		testDatabase(t, e, durabilityDBName, tst)
	case []dbCmd:
		testEngine(t, e, tst)
	}
}
