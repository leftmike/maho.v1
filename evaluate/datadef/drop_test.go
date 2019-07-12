package datadef_test

import (
	"testing"

	"github.com/leftmike/maho/evaluate/datadef"
	"github.com/leftmike/maho/sql"
)

func TestDropTable(t *testing.T) {
	cases := []struct {
		s datadef.DropTable
		r string
	}{
		{
			datadef.DropTable{
				IfExists: false,
				Tables: []sql.TableName{
					{Database: sql.ID("abc"), Schema: sql.ID("def"), Table: sql.ID("ghijk")},
				},
			},
			"DROP TABLE abc.def.ghijk",
		},
		{
			datadef.DropTable{
				IfExists: true,
				Tables: []sql.TableName{
					{Database: sql.ID("abc"), Schema: sql.ID("def"), Table: sql.ID("ghijk")},
					{Table: sql.ID("jkl")},
				},
			},
			"DROP TABLE IF EXISTS abc.def.ghijk, jkl",
		},
	}

	for _, c := range cases {
		if c.s.String() != c.r {
			t.Errorf("DropTable{%v}.String() got %s want %s", c.s, c.s.String(), c.r)
		}
	}
}
