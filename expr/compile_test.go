package expr_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/expr"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

func TestCompile(t *testing.T) {
	cases := []struct {
		s string
		r string
	}{
		{"1 + 2", `"+"(1, 2)`},
		{"1 * 2 + 3 / - 4", `"+"("*"(1, 2), "/"(3, "-"(4)))`},
		{"abs(1 * 2 + 3 / - 4)", `abs("+"("*"(1, 2), "/"(3, "-"(4))))`},
		{"concat('abc', 123, 45.6, true, null)",
			"concat('abc', 123, 45.6, " + sql.TrueString + ", " + sql.NullString + ")"},
	}

	for i, c := range cases {
		p := parser.NewParser(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i))
		e, err := p.ParseExpr()
		if err != nil {
			t.Errorf("ParseExpr(%q) failed with %s", c.s, err)
		}
		r, err := expr.Compile(nil, e)
		if err != nil {
			t.Errorf("expr.Compile(%q) failed with %s", c.s, err)
		}
		if r.String() != c.r {
			t.Errorf("expr.Compile(%q) got %s want %s", c.s, r, c.r)
		}
	}

	fail := []string{
		"abc()",
		"abs()",
		"abs(1, 2)",
		"concat()",
		"concat('abc')",
	}

	for i, f := range fail {
		p := parser.NewParser(strings.NewReader(f), fmt.Sprintf("fail[%d]", i))
		e, err := p.ParseExpr()
		if err != nil {
			t.Errorf("ParseExpr(%q) failed with %s", f, err)
		}
		r, err := expr.Compile(nil, e)
		if err == nil {
			t.Errorf("expr.Compile(%q) did not fail, got %s", f, r)
		}
	}
}
