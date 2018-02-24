package stmt_test

import (
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/stmt"
)

func TestDropTable(t *testing.T) {
	cases := []struct {
		s stmt.DropTable
		r string
	}{
		{
			stmt.DropTable{
				false,
				[]sql.TableName{
					{sql.ID("abc"), sql.ID("defghi")},
				},
			},
			"DROP TABLE abc.defghi",
		},
		{
			stmt.DropTable{
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
