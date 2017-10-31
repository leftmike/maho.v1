package stmt_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"maho/db"
	"maho/parser"
	"maho/query"
	"maho/sql"
	"maho/stmt"
	"maho/test"
)

func TestTableName(t *testing.T) {
	cases := []struct {
		db  sql.Identifier
		tbl sql.Identifier
		r   string
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

func TestDropTable(t *testing.T) {
	s := stmt.DropTable{[]stmt.TableName{{sql.QuotedID("abc"), sql.QuotedID("defghi")}}}
	r := "DROP TABLE abc.defghi"
	if s.String() != r {
		t.Errorf("DropTable{}.String() got %s want %s", s.String(), r)
	}
}

func TestCreateTable(t *testing.T) {
	s := stmt.CreateTable{Table: stmt.TableName{sql.QuotedID("xyz"), sql.QuotedID("abc")}}
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
			sql:  "values (true, 'abcd', 123.456, 789)",
			rows: [][]sql.Value{{true, "abcd", 123.456, int64(789)}},
		},
		{
			sql: "values (1 + 2, 3, 4 - 5), (12, 34, 56.7 * 8)",
			rows: [][]sql.Value{
				{int64(3), int64(3), int64(-1)},
				{int64(12), int64(34), 453.6},
			},
		},
	}

	e, _, err := test.StartEngine("test_insert")
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
		ret, err := stmt.Execute(e)
		if c.fail {
			if err == nil {
				t.Errorf("Execute(%q) did not fail", c.sql)
			}
			continue
		}
		if err != nil {
			t.Errorf("Execute(%q) failed with %s", c.sql, err)
			continue
		}
		rows, ok := ret.(db.Rows)
		if !ok {
			t.Errorf("Execute(%q).(db.Rows) failed", c.sql)
			continue
		}
		dest := make([]sql.Value, len(rows.Columns()))
		for i, r := range c.rows {
			if rows.Next(dest) != nil {
				t.Errorf("Execute(%q) got %d rows; want %d rows", c.sql, i, len(c.rows))
				break
			}
			if !reflect.DeepEqual(dest, r) {
				t.Errorf("Execute(%q)[%d] got %q want %q", c.sql, i, dest, r)
				break
			}
		}
	}
}
