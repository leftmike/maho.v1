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
	cases := []struct {
		stmt datadef.CreateTable
		sql  string
	}{
		{
			stmt: datadef.CreateTable{
				Table: sql.TableName{
					Database: sql.ID("xyz"),
					Schema:   sql.ID("mno"),
					Table:    sql.ID("abc"),
				},
			},
			sql: "CREATE TABLE xyz.mno.abc ()",
		},
		{
			stmt: datadef.CreateTable{
				Table:   sql.TableName{Table: sql.ID("t")},
				Columns: []sql.Identifier{sql.ID("c1"), sql.ID("c2"), sql.ID("c3"), sql.ID("c4")},
				ColumnTypes: []datadef.ColumnType{
					{Type: sql.IntegerType, Size: 4},
					{Type: sql.IntegerType, Size: 4},
					{Type: sql.IntegerType, Size: 4},
					{Type: sql.IntegerType, Size: 4, NotNull: true},
				},
				Constraints: []datadef.Constraint{
					{
						Type:   sql.NotNullConstraint,
						Name:   sql.ID("foreign_1"),
						ColNum: 3,
					},
				},
				ForeignKeys: []datadef.ForeignKey{
					{
						Name:     sql.ID("foreign_2"),
						FKCols:   []sql.Identifier{sql.ID("c1"), sql.ID("c2")},
						RefTable: sql.TableName{Table: sql.ID("t2")},
					},
					{
						Name:     sql.ID("fkey"),
						FKCols:   []sql.Identifier{sql.ID("c3"), sql.ID("c4"), sql.ID("c2")},
						RefTable: sql.TableName{Table: sql.ID("t3")},
						RefCols:  []sql.Identifier{sql.ID("p1"), sql.ID("p2"), sql.ID("p3")},
					},
				},
			},
			sql: "CREATE TABLE t (c1 INT, c2 INT, c3 INT, c4 INT NOT NULL, CONSTRAINT foreign_2 FOREIGN KEY (c1, c2) REFERENCES t2, CONSTRAINT fkey FOREIGN KEY (c3, c4, c2) REFERENCES t3 (p1, p2, p3))",
		},
	}

	for _, c := range cases {
		if c.stmt.String() != c.sql {
			t.Errorf("CreateTable{}.String() got %s want %s", c.stmt.String(), c.sql)
		}
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
		_, err = stmt.Plan(ses, ses.Context(), e, tx)
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
