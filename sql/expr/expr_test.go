package expr_test

import (
	"maho/sql"
	. "maho/sql/expr"
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
			e: &Call{sql.Id("abc"), []Expr{
				&Unary{NegateOp, &Literal{123}},
				&Literal{456},
				&Binary{AddOp,
					Ref{sql.Id("def"), sql.Id("ghi")},
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
