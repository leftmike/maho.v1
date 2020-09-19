package test

import (
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

var (
	schemaSubtests1 = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc1"), fail: true},
		{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc1")},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc1")},
		{fln: fln(), cmd: cmdCommit},
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc1")},
		{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc1")},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc1"), fail: true},
		{fln: fln(), cmd: cmdCommit},
	}

	schemaSubtests2 = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc4")},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc4")},
		{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc4")},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc4"), fail: true},
		{fln: fln(), cmd: cmdCommit},

		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc4"), fail: true},
		{fln: fln(), cmd: cmdCommit},
	}

	schemaSubtests3 = []storeCmd{
		{fln: fln(), cmd: cmdBegin},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc7")},
		{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc7")},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc7"), fail: true},
		{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc7")},
		{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc7")},
		{fln: fln(), cmd: cmdCommit},
	}

	schemaTests = []interface{}{
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
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc-a")},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc-a"), fail: true},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc-a")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListSchemas, list: []string{"public", "sc-a"}},
			{fln: fln(), cmd: cmdCommit},
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc-b")},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc-c")},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc-d")},
			{fln: fln(), cmd: cmdListSchemas,
				list: []string{"public", "sc-a", "sc-b", "sc-c", "sc-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListSchemas,
				list: []string{"public", "sc-a", "sc-b", "sc-c", "sc-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc-a")},
			{fln: fln(), cmd: cmdListSchemas, list: []string{"public", "sc-b", "sc-c", "sc-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListSchemas, list: []string{"public", "sc-b", "sc-c", "sc-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc-e")},
			{fln: fln(), cmd: cmdListSchemas,
				list: []string{"public", "sc-b", "sc-c", "sc-d", "sc-e"}},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListSchemas, list: []string{"public", "sc-b", "sc-c", "sc-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc-c")},
			{fln: fln(), cmd: cmdListSchemas, list: []string{"public", "sc-b", "sc-d"}},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdListSchemas, list: []string{"public", "sc-b", "sc-c", "sc-d"}},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdSetSchema, name: sql.ID("sc-z")},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl"), fail: true},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		schemaSubtests1,
		schemaSubtests1,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc2"), fail: true},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc2"), ifExists: true},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc2"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc2"), fail: true},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc2"), fail: true},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc2"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc3")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc3")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc3")},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc3")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc3")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc3")},
			{fln: fln(), cmd: cmdCommit},
		},
		schemaSubtests2,
		schemaSubtests2,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc5")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc5")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc5")},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc5")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc5")},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc5")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc6")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc6")},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc6")},
			{fln: fln(), cmd: cmdRollback},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc6"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc7")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc7")},
			{fln: fln(), cmd: cmdCommit},
		},
		schemaSubtests3,
		schemaSubtests3,
		schemaSubtests3,
		schemaSubtests3,
		schemaSubtests3,
		schemaSubtests3,
		schemaSubtests3,
		schemaSubtests3,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc7")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc8")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc8"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc8"), fail: true},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc8")},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc8"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc8"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc9")},
			{fln: fln(), cmd: cmdCreateSchema, name: sql.ID("sc10")},
			{fln: fln(), cmd: cmdSetSchema, name: sql.ID("sc10")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl1")},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl2")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc9")},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc10")},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc9")},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc10"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc9"), fail: true},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc10")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdSetSchema, name: sql.ID("sc10")},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl1")},
			{fln: fln(), cmd: cmdDropTable, name: sql.ID("tbl2")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdDropSchema, name: sql.ID("sc10")},
			{fln: fln(), cmd: cmdCommit},
		},
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc9"), fail: true},
			{fln: fln(), cmd: cmdLookupSchema, name: sql.ID("sc10"), fail: true},
			{fln: fln(), cmd: cmdCommit},
		},
	}
)

func RunSchemaTest(t *testing.T, st *storage.Store) {
	t.Helper()

	dbname := sql.ID("schema_test")
	for _, test := range schemaTests {
		runTest(t, st, dbname, test)
	}
}
