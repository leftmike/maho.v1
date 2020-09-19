package test

import (
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

var (
	databaseTests = []interface{}{
		[]dbCmd{
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db1")},
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db1"), fail: true},
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db2")},
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db3")},
			{fln: fln(), cmd: cmdDropDatabase, name: sql.ID("dbtest-not-found"), fail: true},
			{fln: fln(), cmd: cmdDropDatabase, name: sql.ID("dbtest-not-found"), ifExists: true},
			{fln: fln(), cmd: cmdDropDatabase, name: sql.ID("dbtest-db1")},
			{fln: fln(), cmd: cmdDropDatabase, name: sql.ID("dbtest-db1"), fail: true},
		},
		[]dbCmd{
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db4")},
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db1")},
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db3"), fail: true},
			{fln: fln(), cmd: cmdDropDatabase, name: sql.ID("dbtest-db3")},
			{fln: fln(), cmd: cmdDropDatabase, name: sql.ID("dbtest-db3"), fail: true},
			{fln: fln(), cmd: cmdCreateDatabase, name: sql.ID("dbtest-db3")},
		},
	}
)

func RunDatabaseTest(t *testing.T, st *storage.Store) {
	t.Helper()

	dbname := sql.ID("database_test")
	for _, test := range databaseTests {
		runTest(t, st, dbname, test)
	}
}
