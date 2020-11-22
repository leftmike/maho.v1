package expr_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

type compileCtx struct{}

func (_ compileCtx) CompileRef(r expr.Ref) (int, sql.ColumnType, error) {
	if len(r) == 1 && r[0] == sql.ID("f") {
		return 0, sql.ColumnType{Type: sql.FloatType}, nil
	} else if len(r) == 1 && r[0] == sql.ID("i") {
		return 1, sql.ColumnType{Type: sql.IntegerType}, nil
	}
	return -1, sql.ColumnType{}, fmt.Errorf("reference %s not found", r)
}

func TestCompile(t *testing.T) {
	cases := []struct {
		s   string
		r   string
		typ sql.ColumnType
	}{
		{"1 + 2", `"+"(1, 2)`, sql.ColumnType{Type: sql.IntegerType}},
		{"1 * 2 + 3 / - 4", `"+"("*"(1, 2), "/"(3, negate(4)))`,
			sql.ColumnType{Type: sql.IntegerType}},
		{"abs(1 * 2 + 3 / - 4.5)", `abs("+"("*"(1, 2), "/"(3, negate(4.5))))`,
			sql.ColumnType{Type: sql.FloatType}},
		{"concat('abc', 123, 45.6, true, null)",
			"concat('abc', 123, 45.6, " + sql.TrueString + ", " + sql.NullString + ")",
			sql.ColumnType{Type: sql.StringType}},
		{"1 + f", `"+"(1, [0])`, sql.ColumnType{Type: sql.FloatType}},
		{"1.2 + i", `"+"(1.2, [1])`, sql.ColumnType{Type: sql.FloatType}},
		{"1 + i", `"+"(1, [1])`, sql.ColumnType{Type: sql.IntegerType}},
	}

	for i, c := range cases {
		p := parser.NewParser(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i))
		e, err := p.ParseExpr()
		if err != nil {
			t.Errorf("ParseExpr(%q) failed with %s", c.s, err)
		}
		r, ct, err := expr.Compile(nil, nil, nil, compileCtx{}, e)
		if err != nil {
			t.Errorf("expr.Compile(%q) failed with %s", c.s, err)
		}
		if r.String() != c.r {
			t.Errorf("expr.Compile(%q) got %s want %s", c.s, r, c.r)
		}
		if ct.Type != c.typ.Type {
			t.Errorf("expr.Compile(%q) got %s want %s", c.s, ct.Type, c.typ.Type)
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
		r, _, err := expr.Compile(nil, nil, nil, compileCtx{}, e)
		if err == nil {
			t.Errorf("expr.Compile(%q) did not fail, got %s", f, r)
		}
	}
}
