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

func (_ andEqualRefContext) CompileRef(r []sql.Identifier) (int, int, sql.ColumnType, error) {
	var s string
	if len(r) == 1 {
		s = r[0].String()
	} else if len(r) == 2 {
		if r[0].String() != "tbl" {
			return -1, -1, sql.ColumnType{}, errors.New("want tbl.c#")
		}
		s = r[1].String()
	} else {
		return -1, -1, sql.ColumnType{}, errors.New("want tbl.c#")
	}

	if s[0] != 'c' {
		return -1, -1, sql.ColumnType{}, errors.New("want c#")
	}
	i, err := strconv.Atoi(s[1:])
	if err != nil {
		return -1, -1, sql.ColumnType{}, err
	}
	return i, 0, sql.ColumnType{}, nil
}

func TestEqualColExpr(t *testing.T) {
	cases := []struct {
		s  string
		ce []ColExpr
	}{
		{s: "c1 > 12"},
		{s: "c1 and c2"},
		{s: "xxx.c1 == 12"},
		{s: "c1 = 12 and c2 > 12"},
		{
			s:  "12 == c1",
			ce: []ColExpr{{1, -1, sql.Int64Value(12)}},
		},
		{
			s:  "tbl.c2 = 'abc'",
			ce: []ColExpr{{2, -1, sql.StringValue("abc")}},
		},
		{
			s:  "$2 == c3",
			ce: []ColExpr{{3, 2, nil}},
		},
		{
			s:  "c4 = $5",
			ce: []ColExpr{{4, 5, nil}},
		},
		{
			s:  "c1 = 1 and 2 = c2",
			ce: []ColExpr{{1, -1, sql.Int64Value(1)}, {2, -1, sql.Int64Value(2)}},
		},
		{
			s: "c1 = 1 and 2 = c2 and c3 = $3",
			ce: []ColExpr{
				{1, -1, sql.Int64Value(1)},
				{2, -1, sql.Int64Value(2)},
				{3, 3, nil},
			},
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
		ce := EqualColExpr(andEqualRefContext{}, e)
		if !reflect.DeepEqual(c.ce, ce) {
			t.Errorf("EqualColExpr(%q) got %v want %v", c.s, ce, c.ce)
		}
	}
}
