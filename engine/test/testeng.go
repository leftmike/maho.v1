package test

import (
	"context"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/leftmike/maho/db"
	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
)

type session struct{}

func (_ session) Context() context.Context {
	return nil
}

func (_ session) DefaultEngine() string {
	return ""
}

func (_ session) DefaultDatabase() sql.Identifier {
	return 0
}

var (
	int32ColType  = db.ColumnType{Type: sql.IntegerType, Size: 4, NotNull: true}
	int64ColType  = db.ColumnType{Type: sql.IntegerType, Size: 8, NotNull: true}
	boolColType   = db.ColumnType{Type: sql.BooleanType, NotNull: true}
	stringColType = db.ColumnType{Type: sql.CharacterType, Size: 4096, NotNull: true}
)

func testCreateTable(t *testing.T, d engine.Database, tblname sql.Identifier, commit bool) {
	ses := session{}
	tctx := d.Begin()

	_, err := d.LookupTable(ses, tctx, tblname)
	if err == nil {
		t.Errorf("LookupTable(%s) did not fail", tblname)
	}

	err = d.CreateTable(ses, tctx, tblname,
		[]sql.Identifier{sql.ID("col1"), sql.ID("col2"), sql.ID("col3"), sql.ID("col4")},
		[]db.ColumnType{int32ColType, int64ColType, boolColType, stringColType})
	if err != nil {
		t.Errorf("CreateTable(%s) failed with %s", tblname, err)
	}

	_, err = d.LookupTable(ses, tctx, tblname)
	if err != nil {
		t.Errorf("LookupTable(%s) failed with %s", tblname, err)
	}

	if commit || tctx == nil {
		err = d.Commit(ses, tctx)
		if err != nil {
			t.Errorf("Commit() failed with %s", err)
		}

		_, err = d.LookupTable(ses, tctx, tblname)
		if err != nil {
			t.Errorf("LookupTable(%s) failed with %s", tblname, err)
		}
	} else {
		err = d.Rollback(tctx)
		if err != nil {
			t.Errorf("Rollback() failed with %s", err)
		}

		_, err = d.LookupTable(ses, tctx, tblname)
		if err == nil {
			t.Errorf("LookupTable(%s) did not fail", tblname)
		}
	}
}

func testDropTable(t *testing.T, d engine.Database, tblname sql.Identifier, commit bool) {
	ses := session{}
	tctx := d.Begin()

	err := d.DropTable(ses, tctx, tblname, false)
	if err != nil {
		t.Errorf("DropTable(%s) failed with %s", tblname, err)
	}
	_, err = d.LookupTable(ses, tctx, tblname)
	if err == nil {
		t.Errorf("LookupTable(%s) did not fail", tblname)
	}

	if commit || tctx == nil {
		err = d.Commit(ses, tctx)
		if err != nil {
			t.Errorf("Commit() failed with %s", err)
		}

		_, err = d.LookupTable(ses, tctx, tblname)
		if err == nil {
			t.Errorf("LookupTable(%s) did not fail", tblname)
		}
	} else {
		err = d.Rollback(tctx)
		if err != nil {
			t.Errorf("Rollback() failed with %s", err)
		}

		_, err = d.LookupTable(ses, tctx, tblname)
		if err != nil {
			t.Errorf("LookupTable(%s) failed with %s", tblname, err)
		}
	}
}

func RunDatabaseTest(t *testing.T, e engine.Engine) {
	t.Helper()

	ses := session{}

	d, err := e.CreateDatabase(sql.ID("database_test"), filepath.Join("testdata", "database_test"),
		nil)
	if err != nil {
		t.Fatal(err)
	}

	_ = d.Message()

	tctx := d.Begin()
	err = d.DropTable(ses, tctx, sql.ID("not_table"), true)
	if err != nil {
		t.Errorf("DropTable(\"not_table\", true) failed with %s", err)
	}
	err = d.DropTable(ses, tctx, sql.ID("not_table"), false)
	if err == nil {
		t.Errorf("DropTable(\"not_table\", false) did not fail")
	}
	err = d.Commit(ses, tctx)
	if err != nil {
		t.Errorf("Commit() failed with %s", err)
	}

	names := []string{"tbl-a", "tbl-b", "tbl-c", "tbl-d"}
	for _, n := range names {
		testCreateTable(t, d, sql.ID(n), true)
	}

	tctx = d.Begin()
	entries, err := d.ListTables(ses, tctx)
	if err != nil {
		t.Errorf("ListTables() failed with %s", err)
	} else {
		var ret []string
		for _, te := range entries {
			ret = append(ret, te.Name.String())
		}
		sort.Strings(ret)
		if !reflect.DeepEqual(names, ret) {
			t.Errorf("ListTables() got %v want %v", ret, names)
		}
	}
	err = d.Commit(ses, tctx)
	if err != nil {
		t.Errorf("Commit() failed with %s", err)
	}

	testCreateTable(t, d, sql.ID("tbl1"), true)
	testDropTable(t, d, sql.ID("tbl1"), true)

	testCreateTable(t, d, sql.ID("tbl2"), false)

	testCreateTable(t, d, sql.ID("tbl3"), true)
	testDropTable(t, d, sql.ID("tbl3"), false)
}

func RunTableTest(t *testing.T, e engine.Engine) {
	t.Helper()

	ses := session{}

	d, err := e.CreateDatabase(sql.ID("database_test"), filepath.Join("testdata", "database_test"),
		nil)
	if err != nil {
		t.Fatal(err)
	}

	tblname := sql.ID("tbl1")
	testCreateTable(t, d, tblname, true)

	tctx := d.Begin()
	tbl, err := d.LookupTable(ses, tctx, tblname)
	if err != nil {
		t.Errorf("LookupTable(%s) failed with %s", tblname, err)
	}

	_ = tbl // XXX
}
