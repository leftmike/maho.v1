package sql_test

import (
	"testing"

	"github.com/leftmike/maho/sql"
)

func TestTableName(t *testing.T) {
	cases := []struct {
		db  sql.Identifier
		sc  sql.Identifier
		tbl sql.Identifier
		r   string
	}{
		{tbl: sql.ID("abc"), r: "abc"},
		{sql.ID("abcd"), sql.ID("ef"), sql.ID("ghijk"), "abcd.ef.ghijk"},
	}

	for _, c := range cases {
		tn := sql.TableName{Database: c.db, Schema: c.sc, Table: c.tbl}
		if tn.String() != c.r {
			t.Errorf("TableName(%s.%s).String() got %s want %s", c.db, c.tbl, tn.String(), c.r)
		}
	}
}

func TestSchemaName(t *testing.T) {
	cases := []struct {
		db  sql.Identifier
		scm sql.Identifier
		r   string
	}{
		{scm: sql.ID("abc"), r: "abc"},
		{sql.ID("abcd"), sql.ID("efghijk"), "abcd.efghijk"},
	}

	for _, c := range cases {
		sn := sql.SchemaName{Database: c.db, Schema: c.scm}
		if sn.String() != c.r {
			t.Errorf("SchemaName(%s.%s).String() got %s want %s", c.db, c.scm, sn.String(), c.r)
		}
	}
}
