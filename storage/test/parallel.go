package test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

func RunParallelTest(t *testing.T, st *storage.Store) {
	t.Helper()

	dbname := sql.ID("parallel_test")
	err := st.CreateDatabase(sql.ID("parallel_test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	testDatabase(t, st, dbname,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl")},
			{fln: fln(), cmd: cmdCommit},
		})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i, r int) {
			defer wg.Done()

			for j := 0; j < r; j++ {
				testDatabase(t, st, dbname,
					[]storeCmd{
						{fln: fln(), cmd: cmdBegin},
						{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl")},
						{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(i*r + j),
							sql.Int64Value(j), sql.StringValue(fmt.Sprintf("row %d.%d", i, j))}},
						{fln: fln(), cmd: cmdCommit},
					})
			}

			for j := 0; j < r; j++ {
				testDatabase(t, st, dbname,
					[]storeCmd{
						{fln: fln(), cmd: cmdBegin},
						{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl")},
						{fln: fln(), cmd: cmdUpdate, rowID: i*r + j,
							updates: []sql.ColumnUpdate{
								{Column: 1, Value: sql.Int64Value(j * j)},
							},
						},
						{fln: fln(), cmd: cmdCommit},
					})
			}
		}(i, 100)
	}
	wg.Wait()
}
