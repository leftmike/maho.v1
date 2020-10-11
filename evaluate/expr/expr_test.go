package expr_test

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	. "github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/sql"
)

func TestExpr(t *testing.T) {
	cases := []struct {
		e Expr
		s string
	}{
		{
			e: &Binary{
				Op:    DivideOp,
				Left:  &Unary{Op: NegateOp, Expr: Int64Literal(123)},
				Right: Int64Literal(456),
			},
			s: "((- 123) / 456)"},
		{
			e: &Call{
				Name: sql.ID("abc"),
				Args: []Expr{
					&Unary{Op: NegateOp, Expr: Int64Literal(123)},
					Param{Num: 3},
					Int64Literal(456),
					&Binary{Op: AddOp,
						Left:  Ref{sql.ID("def"), sql.ID("ghi")},
						Right: Int64Literal(789),
					},
				},
			},
			s: "abc((- 123), $3, 456, (def.ghi + 789))",
		},
	}

	for _, c := range cases {
		if c.e.String() != c.s {
			t.Errorf("%q.String() != %q", c.e.String(), c.s)
		}
	}
}

type andEqualRefContext struct{}

func (_ andEqualRefContext) CompileRef(r Ref) (int, error) {
	var s string
	if len(r) == 1 {
		s = r[0].String()
	} else if len(r) == 2 {
		if r[0].String() != "tbl" {
			return 0, errors.New("want tbl.c#")
		}
		s = r[1].String()
	} else {
		return 0, errors.New("want tbl.c#")
	}

	if s[0] != 'c' {
		return 0, errors.New("want c#")
	}
	i, err := strconv.Atoi(s[1:])
	if err != nil {
		return 0, err
	}
	return i, nil
}

func TestAndEqualRef(t *testing.T) {
	cases := []struct {
		s  string
		cv []ColVal
	}{
		{s: "c1 > 12"},
		{s: "c1 and c2"},
		{s: "xxx.c1 == 12"},
		{s: "c1 = 12 and c2 > 12"},
		{
			s:  "12 == c1",
			cv: []ColVal{{1, Int64Literal(12)}},
		},
		{
			s:  "tbl.c2 = 'abc'",
			cv: []ColVal{{2, StringLiteral("abc")}},
		},
		{
			s:  "$2 == c3",
			cv: []ColVal{{3, Param{2}}},
		},
		{
			s:  "c4 = $5",
			cv: []ColVal{{4, Param{5}}},
		},
		{
			s:  "c1 = 1 and 2 = c2",
			cv: []ColVal{{1, Int64Literal(1)}, {2, Int64Literal(2)}},
		},
		{
			s:  "c1 = 1 and 2 = c2 and c3 = $3",
			cv: []ColVal{{1, Int64Literal(1)}, {2, Int64Literal(2)}, {3, Param{3}}},
		},
		{s: "c1 = 1 and 2 = c2 or c3 = $3"},
		{s: "c1 = 1 and 2 = c2 and c3 > $3"},
	}

	for i, c := range cases {
		p := parser.NewParser(strings.NewReader(c.s), fmt.Sprintf("%d", i))
		e, err := p.ParseExpr()
		if err != nil {
			t.Errorf("ParseExpr(%q) failed with %s", c.s, err)
		}
		cv := AndEqualRef(andEqualRefContext{}, e)
		if !reflect.DeepEqual(c.cv, cv) {
			t.Errorf("AndEqualRef(%q) got %v want %v", c.s, cv, c.cv)
		}
	}
}
