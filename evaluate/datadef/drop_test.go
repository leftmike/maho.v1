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
					{Database: sql.ID("abc"), Table: sql.ID("defghi")},
				},
			},
			"DROP TABLE abc.defghi",
		},
		{
			datadef.DropTable{
				IfExists: true,
				Tables: []sql.TableName{
					{Database: sql.ID("abc"), Table: sql.ID("defghi")},
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
