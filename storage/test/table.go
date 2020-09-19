package test

import (
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

var (
	tableTests = []interface{}{
		"createDatabase",
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc-a"), fail: true},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc-a")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc-a")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a"), fail: true},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl-a")},
			{fln: fln(), cmd: cmdCommit},
		},
	}
)

func RunTableTest(t *testing.T, st *storage.Store) {
	t.Helper()

	dbname := sql.ID("table_test")
	for _, test := range tableTests {
		runTest(t, st, dbname, test)
	}
}
