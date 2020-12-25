package test

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
)

func incColumn(t *testing.T, st *storage.Store, tx engine.Transaction, tdx uint64, i int,
	tn sql.TableName) bool {

	var ctx context.Context

	tbl, _, err := st.LookupTable(ctx, tx, tn)
	if err != nil {
		t.Fatalf("LookupTable(%s) failed with %s", tn, err)
	}
	rows, err := tbl.Rows(ctx, nil, nil)
	if err != nil {
		t.Fatalf("table.Rows() failed with %s", err)
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			t.Errorf("rows.Close() failed with %s", err)
		}
	}()

	for {
		dest, err := rows.Next(ctx)
		if err != nil {
			if err != io.EOF {
				t.Errorf("rows.Next() failed with %s", err)
			}
			t.Fatalf("rows.Next() row not found")
		}
		if i64, ok := dest[0].(sql.Int64Value); ok && int(i64) == i {
			v := int(dest[1].(sql.Int64Value))
			err = rowUpdate(ctx, rows,
				[]sql.ColumnUpdate{{Column: 1, Value: sql.Int64Value(v + 1)}}, dest)
			if err == nil {
				//fmt.Printf("%d: %d -> %d\n", tdx, i, v+1)
				return true
			}
			break
		}
	}

	return false
}

func RunStressTest(t *testing.T, st *storage.Store) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Helper()

	dbname := sql.ID("stress_test")
	err := st.CreateDatabase(dbname, nil)
	if err != nil {
		t.Fatal(err)
	}

	testDatabase(t, st, dbname,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdCreateTable, name: sql.ID("tbl")},
			{fln: fln(), cmd: cmdCommit},
		})

	const rcnt = 100

	for i := 0; i < rcnt; i++ {
		testDatabase(t, st, dbname,
			[]storeCmd{
				{fln: fln(), cmd: cmdBegin},
				{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl")},
				{fln: fln(), cmd: cmdInsert, row: []sql.Value{sql.Int64Value(i),
					sql.Int64Value(0), sql.StringValue(fmt.Sprintf("row %d", i))}},
				{fln: fln(), cmd: cmdCommit},
			})
	}
	const tcnt = 20

	var wg sync.WaitGroup
	for tdx := uint64(0); tdx < tcnt; tdx++ {
		wg.Add(1)
		go func(tdx uint64, r int) {
			defer wg.Done()

			var ctx context.Context
			name := sql.ID("tbl")
			i := 0
			for i < r {
				updated := false
				for !updated {
					tx := st.Begin(tdx)
					updated = incColumn(t, st, tx, tdx, i, sql.TableName{dbname, sql.PUBLIC, name})
					if updated {
						err := tx.Commit(ctx)
						if err == nil {
							i += 1
						}
					} else {
						err := tx.Rollback()
						if err != nil {
							t.Errorf("Rollback() failed with %s", err)
						}
					}
				}
			}
		}(tdx, rcnt)
	}
	wg.Wait()

	var values [][]sql.Value
	for i := 0; i < rcnt; i++ {
		values = append(values, []sql.Value{sql.Int64Value(i), sql.Int64Value(tcnt),
			sql.StringValue(fmt.Sprintf("row %d", i))})
	}

	testDatabase(t, st, dbname,
		[]storeCmd{
			{fln: fln(), cmd: cmdBegin},
			{fln: fln(), cmd: cmdLookupTable, name: sql.ID("tbl")},
			{fln: fln(), cmd: cmdRows, values: values},
			{fln: fln(), cmd: cmdCommit},
		})
}
