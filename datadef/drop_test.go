package datadef_test

import (
	"testing"

	"github.com/leftmike/maho/datadef"
	"github.com/leftmike/maho/sql"
)

func TestDropTable(t *testing.T) {
	cases := []struct {
		s datadef.DropTable
		r string
	}{
		{
			datadef.DropTable{
				false,
				[]sql.TableName{
					{sql.ID("abc"), sql.ID("defghi")},
				},
			},
			"DROP TABLE abc.defghi",
		},
		{
			datadef.DropTable{
				true,
				[]sql.TableName{
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