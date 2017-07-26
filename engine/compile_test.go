package engine

import (
	"fmt"
	"maho/sql"
	"maho/sql/parser"
	"strings"
	"testing"
)

func TestCompile(t *testing.T) {
	cases := []struct {
		s string
		r string
	}{
		{"1 + 2", `"+"(1, 2)`},
		{"abs(1 * 2 + 3 / - 4)", `abs("+"("*"(1, 2), "/"(3, "-"(4))))`},
		{"concat('abc', 123, 45.6, true, null)",
			"concat('abc', 123, 45.6, " + sql.TrueString + ", " + sql.NullString + ")"},
	}

	for i, c := range cases {
		var p parser.Parser
		p.Init(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i))
		e, err := p.ParseExpr()
		if err != nil {
			t.Errorf("ParseExpr(%q) failed with %s", c.s, err)
		}
		r, err := Compile(nil, e)
		if err != nil {
			t.Errorf("Compile(%q) failed with %s", c.s, err)
		}
		if r.String() != c.r {
			t.Errorf("Compile(%q) got %s want %s", c.s, r, c.r)
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
		var p parser.Parser
		p.Init(strings.NewReader(f), fmt.Sprintf("fail[%d]", i))
		e, err := p.ParseExpr()
		if err != nil {
			t.Errorf("ParseExpr(%q) failed with %s", f, err)
		}
		r, err := Compile(nil, e)
		if err == nil {
			t.Errorf("Compile(%q) did not fail, got %s", f, r)
		}
	}
}

func TestFuncs(t *testing.T) {
	for op, cf := range opFuncs {
		if op == sql.NegateOp || op == sql.NotOp {
			if cf.minArgs != 1 {
				t.Errorf("opFuncs[%s].minArgs got %d want 1", op, cf.minArgs)
			}
			if cf.maxArgs != 1 {
				t.Errorf("opFuncs[%s].maxArgs got %d want 1", op, cf.maxArgs)
			}
		} else {
			if cf.minArgs != 2 {
				t.Errorf("opFuncs[%s].minArgs got %d want 2", op, cf.minArgs)
			}
			if cf.maxArgs != 2 {
				t.Errorf("opFuncs[%s].maxArgs got %d want 2", op, cf.maxArgs)
			}
		}

		n := fmt.Sprintf(`"%s"`, op)
		if cf.name != n {
			t.Errorf("opFuncs[%s].name got %s want %s", op, cf.name, n)
		}
	}

	for id, cf := range idFuncs {
		if cf.minArgs < 0 {
			t.Errorf("idFuncs[%s].minArgs < 0; got %d", id, cf.minArgs)
		}
		if cf.maxArgs < cf.minArgs {
			t.Errorf("idFuncs[%s].maxArgs < minArgs; got %d < %d", id, cf.maxArgs, cf.minArgs)
		}
		if cf.name != id.String() {
			t.Errorf("idFuncs[%s].name got %s want %s", id, cf.name, id)
		}
	}
}
