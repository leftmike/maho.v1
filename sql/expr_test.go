package sql_test

import (
	. "maho/sql"
	"testing"
)

func TestExpr(t *testing.T) {
	cases := []struct {
		e Expr
		s string
	}{
		{
			e: &Binary{DivideOp,
				&Unary{NegateOp, &Literal{123}},
				&Literal{456}},
			s: "((- 123) / 456)"},
		{
			e: &Call{ID("abc"), []Expr{
				&Unary{NegateOp, &Literal{123}},
				&Literal{456},
				&Binary{AddOp,
					Ref{ID("def"), ID("ghi")},
					&Literal{789}}}},
			s: "abc((- 123), 456, (def.ghi + 789))",
		},
	}

	for _, c := range cases {
		if c.e.String() != c.s {
			t.Errorf("%q.String() != %q", c.e.String(), c.s)
		}
	}
}
