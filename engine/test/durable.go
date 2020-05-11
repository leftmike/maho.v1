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
	durabilityTests = [][]engCmd{
		// TableLifecycleTest
		nil, // cleanDir
		{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a"), fail: true},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdCommit},
		},
		/*
			{
				{fln: fln(), cmd: cmdBegin},
				{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
				{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-a"), fail: true},
				{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
				{fln: fln(), cmd: cmdCommit},
			},
		*/
		/*
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-a"}},
			{fln: fln(), cmd: cmdCommit},
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-b")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-c")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-d")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-a", "tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},

			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-a", "tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},

			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},

			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},

			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-e")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d", "tbl-e"}},
			{fln: fln(), cmd: cmdRollback},

			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},

			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl-c")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-d"}},
			{fln: fln(), cmd: cmdRollback},

			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		*/
	}
)

func RunDurabilityTest(t *testing.T, cleanDir func() error) {
	for testNum, dt := range durabilityTests {
		if dt == nil {
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

	dbname := sql.ID("durability_test")
	err = e.CreateDatabase(dbname, nil)
	if err != nil {
		t.Fatal(err)
	}

	testDatabase(t, e, dbname, durabilityTests[testNum])
}
