package expr_test

import (
	"testing"

	. "maho/expr"
	"maho/sql"
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
			e: &Call{sql.ID("abc"), []Expr{
				&Unary{NegateOp, &Literal{123}},
				&Literal{456},
				&Binary{AddOp,
					Ref{sql.ID("def"), sql.ID("ghi")},
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
