package datadef_test

import (
	"strings"
	"testing"

	"github.com/leftmike/maho/evaluate/datadef"
	"github.com/leftmike/maho/evaluate/test"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

func TestCreateTableString(t *testing.T) {
	s := datadef.CreateTable{
		Table: sql.TableName{
			Database: sql.ID("xyz"),
			Schema:   sql.ID("mno"),
			Table:    sql.ID("abc"),
		},
	}
	r := "CREATE TABLE xyz.mno.abc ()"
	if s.String() != r {
		t.Errorf("CreateTable{}.String() got %s want %s", s.String(), r)
	}
}

func TestCreateTablePlan(t *testing.T) {
	cases := []struct {
		sql  string
		fail bool
	}{
		{
			sql: "create table t (c1 int check(c1 > 10), c2 int check(c2 < 10))",
		},
		{
			sql:  "create table t (c1 int check(c2 > 10), c2 int)",
			fail: true,
		},
		{
			sql: "create table t (check(c1 < c2), c1 int, c2 int)",
		},
		{
			sql:  "create table t (check(c3 < c2), c1 int, c2 int)",
			fail: true,
		},
		{
			sql: "create table t (c1 int default 10 + 5, c2 int)",
		},
		{
			sql:  "create table t (c1 int default 10 + c2, c2 int)",
			fail: true,
		},
	}

	e, ses := test.StartSession(t)
	for _, c := range cases {
		p := parser.NewParser(strings.NewReader(c.sql), "test")
		stmt, err := p.Parse()
		if err != nil {
			t.Fatal(err)
		}
		tx := e.Begin(0)
		_, err = stmt.Plan(ses, tx)
		if err == nil {
			if c.fail {
				t.Errorf("Plan(%q) did not fail", c.sql)
			}
		} else if !c.fail {
			t.Errorf("Plan(%q) failed with %s", c.sql, err)
		}
		tx.Rollback()
	}
}
