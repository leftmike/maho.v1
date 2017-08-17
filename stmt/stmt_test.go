package stmt_test

import (
	"testing"

	"maho/sql"
	"maho/stmt"
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
		Table: stmt.TableAlias{
			stmt.TableName{sql.QuotedID("db"), sql.QuotedID("tbl")},
			sql.QuotedID("alias"),
		},
	}
	r := "SELECT * FROM db.tbl AS alias"
	if s.String() != r {
		t.Errorf("Select{}.String() got %s want %s", s.String(), r)
	}
}
