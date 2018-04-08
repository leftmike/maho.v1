package sql_test

import (
	"testing"

	"github.com/leftmike/maho/sql"
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
		tn := sql.TableName{Database: c.db, Table: c.tbl}
		if tn.String() != c.r {
			t.Errorf("TableName(%s.%s).String() got %s want %s", c.db, c.tbl, tn.String(), c.r)
		}
	}
}
