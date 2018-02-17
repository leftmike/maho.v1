package stmt_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/plan"
	"github.com/leftmike/maho/query"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/stmt"
	"github.com/leftmike/maho/testutil"
)

func TestTableName(t *testing.T) {
	cases := []struct {
		db  sql.Identifier
		tbl sql.Identifier
		r   string
	}{
		{tbl: sql.ID("abc"), r: "abc"},
		{sql.ID("abcd"), sql.ID("efghijk"), "abcd.efghijk"},
	}

	for _, c := range cases {
		tn := stmt.TableName{c.db, c.tbl}
		if tn.String() != c.r {
			t.Errorf("TableName(%s.%s).String() got %s want %s", c.db, c.tbl, tn.String(), c.r)
		}
	}
}

func TestDropTable(t *testing.T) {
	cases := []struct {
		s stmt.DropTable
		r string
	}{
		{
			stmt.DropTable{
				false,
				[]stmt.TableName{
					{sql.ID("abc"), sql.ID("defghi")},
				},
			},
			"DROP TABLE abc.defghi",
		},
		{
			stmt.DropTable{
				true,
				[]stmt.TableName{
					{sql.ID("abc"), sql.ID("defghi")},
					{Table: sql.ID("jkl")},
				},
			},
			"DROP TABLE IF EXISTS abc.defghi, jkl",
		},
	}

	for _, c := range cases {
		if c.s.String() != c.r {
			t.Errorf("DropTable{%v}.String() got %s want %s", c.s, c.s.String(), c.r)
		}
	}
}

func TestCreateTable(t *testing.T) {
	s := stmt.CreateTable{Table: stmt.TableName{sql.ID("xyz"), sql.ID("abc")}}
	r := "CREATE TABLE xyz.abc ()"
	if s.String() != r {
		t.Errorf("CreateTable{}.String() got %s want %s", s.String(), r)
	}
}

func TestInsertValues(t *testing.T) {
	s := stmt.InsertValues{Table: stmt.TableName{sql.QuotedID("left"), sql.QuotedID("right")}}
	r := "INSERT INTO left.right VALUES"
	if s.String() != r {
		t.Errorf("InsertValues{}.String() got %s want %s", s.String(), r)
	}
}

func TestSelect(t *testing.T) {
	s := stmt.Select{
		From: query.FromTableAlias{
			Database: sql.ID("db"),
			Table:    sql.ID("tbl"),
			Alias:    sql.ID("alias"),
		},
	}
	r := "SELECT * FROM db.tbl AS alias"
	if s.String() != r {
		t.Errorf("Select{}.String() got %s want %s", s.String(), r)
	}
}

func TestValues(t *testing.T) {
	cases := []struct {
		sql  string
		fail bool
		rows [][]sql.Value
	}{
		{
			sql: "values (true, 'abcd', 123.456, 789)",
			rows: [][]sql.Value{{sql.BoolValue(true), sql.StringValue("abcd"),
				sql.Float64Value(123.456), sql.Int64Value(789)}},
		},
		{
			sql: "values (1 + 2, 3, 4 - 5), (12, 34, 56.7 * 8)",
			rows: [][]sql.Value{
				{sql.Int64Value(3), sql.Int64Value(3), sql.Int64Value(-1)},
				{sql.Int64Value(12), sql.Int64Value(34), sql.Float64Value(453.6)},
			},
		},
	}

	e, _, err := testutil.StartEngine("test_insert")
	if err != nil {
		t.Fatal(err)
	}

	for i, c := range cases {
		p := parser.NewParser(strings.NewReader(c.sql), fmt.Sprintf("tests[%d]", i))
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) failed with %s", c.sql, err)
			continue
		}
		ret, err := stmt.Plan(e)
		if c.fail {
			if err == nil {
				t.Errorf("Plan(%q) did not fail", c.sql)
			}
			continue
		}
		if err != nil {
			t.Errorf("Plan(%q) failed with %s", c.sql, err)
			continue
		}
		rows, ok := ret.(plan.Rows)
		if !ok {
			t.Errorf("Plan(%q).(plan.Rows) failed", c.sql)
			continue
		}
		dest := make([]sql.Value, len(rows.Columns()))
		for i, r := range c.rows {
			if rows.Next(dest) != nil {
				t.Errorf("Plan(%q).Rows() got %d rows; want %d rows", c.sql, i, len(c.rows))
				break
			}
			if !reflect.DeepEqual(dest, r) {
				t.Errorf("Plan(%q).Rows()[%d] got %q want %q", c.sql, i, dest, r)
				break
			}
		}
	}
}
