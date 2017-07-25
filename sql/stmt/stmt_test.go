package stmt_test

import (
	"errors"
	"maho/sql"
	"maho/sql/stmt"
	"testing"
)

func TestTableName(t *testing.T) {
	cases := []struct {
		db sql.Identifier
		tbl sql.Identifier
		r string
	}{
		{tbl: sql.QuotedID("abc"), r: "abc"},
		{sql.QuotedID("abcd"), sql.QuotedID("efghijk"), "abcd.efghijk"},
	}

	for _, c := range cases {
		tn := stmt.TableName{c.db, c.tbl}
		if tn.String() != c.r {
			t.Errorf("TableName(%s.%s).String() got %s want %s", c.db, c.tbl, tn.String(), c.r)
		}
	}
}

type mockExecuter struct {
	createTable bool
	dropTable bool
	insertValues bool
	selectStmt bool
	ret interface{}
	err error
}

func (me *mockExecuter) CreateTable(stmt *stmt.CreateTable) (interface{}, error) {
	me.createTable = true
	return me.ret, me.err
}

func (me *mockExecuter) DropTable(stmt *stmt.DropTable) (interface{}, error) {
	me.dropTable = true
	return me.ret, me.err
}

func (me *mockExecuter) InsertValues(stmt *stmt.InsertValues) (interface{}, error) {
	me.insertValues = true
	return me.ret, me.err
}

func (me *mockExecuter) Select(stmt *stmt.Select) (interface{}, error) {
	me.selectStmt = true
	return me.ret, me.err
}

func TestDropTable(t *testing.T) {
	s := stmt.DropTable{[]stmt.TableName{{sql.QuotedID("abc"), sql.QuotedID("defghi")}}}
	r := "DROP TABLE abc.defghi"
	if s.String() != r {
		t.Errorf("DropTable{}.String() got %s want %s", s.String(), r)
	}

	me := mockExecuter{ret: new([]byte), err: errors.New("mock")}
	re := me
	re.dropTable = true

	me.ret, me.err = s.Dispatch(&me)
	if me != re {
		t.Errorf("DropTable{}.Dispatch() got %+v want %+v", me, re)
	}
}

func TestCreateTable(t *testing.T) {
	s := stmt.CreateTable{Table: stmt.TableName{sql.QuotedID("xyz"), sql.QuotedID("abc")}}
	r := "CREATE TABLE xyz.abc ()"
	if s.String() != r {
		t.Errorf("CreateTable{}.String() got %s want %s", s.String(), r)
	}

	me := mockExecuter{ret: new([]byte), err: errors.New("mock")}
	re := me
	re.createTable = true

	me.ret, me.err = s.Dispatch(&me)
	if me != re {
		t.Errorf("CreateTable{}.Dispatch() got %+v want %+v", me, re)
	}
}

func TestInsertValues(t *testing.T) {
	s := stmt.InsertValues{Table: stmt.TableName{sql.QuotedID("left"), sql.QuotedID("right")}}
	r := "INSERT INTO left.right VALUES"
	if s.String() != r {
		t.Errorf("InsertValues{}.String() got %s want %s", s.String(), r)
	}

	me := mockExecuter{ret: new([]byte), err: errors.New("mock")}
	re := me
	re.insertValues = true

	me.ret, me.err = s.Dispatch(&me)
	if me != re {
		t.Errorf("InsertValues{}.Dispatch() got %+v want %+v", me, re)
	}
}

func TestSelect(t *testing.T) {
	s := stmt.Select{
		Table: stmt.AliasTableName{
			stmt.TableName{sql.QuotedID("db"), sql.QuotedID("tbl")},
			sql.QuotedID("alias"),
		},
	}
	r := "SELECT * FROM db.tbl AS alias"
	if s.String() != r {
		t.Errorf("Select{}.String() got %s want %s", s.String(), r)
	}

	me := mockExecuter{ret: new([]byte), err: errors.New("mock")}
	re := me
	re.selectStmt = true

	me.ret, me.err = s.Dispatch(&me)
	if me != re {
		t.Errorf("Select{}.Dispatch() got %+v want %+v", me, re)
	}
}
