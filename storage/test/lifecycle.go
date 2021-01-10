package test

import (
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

var (
	tableLifecycleSubtests1 = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1"), fail: true},
		{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl1")},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
		{fln: fln(), cmd: cmdCommit},
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1")},
		{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl1")},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl1"), fail: true},
		{fln: fln(), cmd: cmdCommit},
	}
	tableLifecycleSubtests2 = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl4")},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl4")},
		{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl4")},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl4"), fail: true},
		{fln: fln(), cmd: cmdCommit},

		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl4"), fail: true},
		{fln: fln(), cmd: cmdCommit},
	}
	tableLifecycleSubtests3 = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl7")},
		{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl7")},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl7"), fail: true},
		{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl7")},
		{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl7")},
		{fln: fln(), cmd: cmdCommit},
	}

	tableLifecycleTests = []interface{}{
		"createDatabase",
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a"), fail: true},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-a"), fail: true},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
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
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-a", "tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-e")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d", "tbl-e"}},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl-c")},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-d"}},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListTables, list: []string{"tbl-b", "tbl-c", "tbl-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		tableLifecycleSubtests1,
		tableLifecycleSubtests1,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl2"), fail: true},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl2"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl3")},
			{fln: fln(), cmd: cmdCommit},
		},
		tableLifecycleSubtests2,
		tableLifecycleSubtests2,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl5")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl5")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl5")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl5")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl5")},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl5")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl6")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl6")},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl6")},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl6"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl7")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl7")},
			{fln: fln(), cmd: cmdCommit},
		},
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		tableLifecycleSubtests3,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl7")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl8")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl8"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl8"), fail: true},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl8")},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl8"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl8"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
	}
)

func RunTableLifecycleTest(t *testing.T, st *storage.Store) {
	t.Helper()

	dbname := sql.ID("tbl_lifecycle_test")
	for _, test := range tableLifecycleTests {
		runTest(t, st, dbname, test)
	}
}
