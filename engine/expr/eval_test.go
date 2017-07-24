package expr_test

import (
	"fmt"
	. "maho/engine/expr"
	"maho/sql"
	"maho/sql/parser"
	"strings"
	"testing"
)

func TestEval(t *testing.T) {
	cases := []struct {
		s string
		r string
	}{
		{"1 + null", "NULL"},
		{"null + 2.3", "NULL"},
		{"123 + 456", "579"},
		{"123 + 4.56", fmt.Sprintf("%v", 123+4.56)},
		{"12.3 + 456", fmt.Sprintf("%v", 12.3+456)},
		{"1.23 + 45.6", fmt.Sprintf("%v", 1.23+45.6)},

		{"1 / null", "NULL"},
		{"null / 2.3", "NULL"},
		{"456 / 123", "3"},
		{"123 / 45.6", fmt.Sprintf("%v", 123/45.6)},
		{"123.45 / 6", fmt.Sprintf("%v", 123.45/6)},
		{"12.3 / 45.6", fmt.Sprintf("%v", 12.3/45.6)},

		{"1 * null", "NULL"},
		{"null * 2.3", "NULL"},
		{"456 * 123", "56088"},
		{"123 * 45.6", fmt.Sprintf("%v", 123*45.6)},
		{"123.45 * 6", fmt.Sprintf("%v", 123.45*6)},
		{"12.3 * 45.6", fmt.Sprintf("%v", 12.3*45.6)},

		{"- null", "NULL"},
		{"- (1 + null)", "NULL"},
		{"- 123", "-123"},
		{"- 123.456", "-123.456"},
		{"- (123 + 456)", "-579"},
		{"- (1.23 + 4.5)", "-5.73"},

		{"1 - null", "NULL"},
		{"null - 2.3", "NULL"},
		{"456 - 123", "333"},
		{"123 - 45.6", fmt.Sprintf("%v", 123-45.6)},
		{"123.45 - 6", fmt.Sprintf("%v", 123.45-6)},
		{"12.3 - 45.6", fmt.Sprintf("%v", 12.3-45.6)},

		{"1 & null", "NULL"},
		{"null & 2", "NULL"},
		{"15 & 2", "2"},
		{"12345 & 67890", fmt.Sprintf("%v", 12345&67890)},

		{"1 << null", "NULL"},
		{"null << 2", "NULL"},
		{"1 << 2", "4"},
		{"123 << 4", fmt.Sprintf("%v", 123<<4)},

		{"1 % null", "NULL"},
		{"null % 2", "NULL"},
		{"15 % 4", "3"},
		{"12345 % 67", fmt.Sprintf("%v", 12345%67)},

		{"1 | null", "NULL"},
		{"null | 2", "NULL"},
		{"1 | 2", "3"},
		{"12345 | 67890", fmt.Sprintf("%v", 12345|67890)},

		{"1 >> null", "NULL"},
		{"null >> 2", "NULL"},
		{"16 >> 2", "4"},
		{"123456789 >> 4", fmt.Sprintf("%v", 123456789>>4)},

		{"null AND true", "NULL"},
		{"false AND null", "NULL"},
		{"false AND true", "false"},
		{"true AND false", "false"},
		{"true AND true", "true"},
		{"false AND false", "false"},

		{"null OR true", "NULL"},
		{"false OR null", "NULL"},
		{"false OR true", "true"},
		{"true OR false", "true"},
		{"true OR true", "true"},
		{"false OR false", "false"},

		{"NOT null", "NULL"},
		{"NOT false", "true"},
		{"NOT true", "false"},

		{"abs(null)", "NULL"},
		{"abs(123)", "123"},
		{"abs(-123)", "123"},
		{"abs(12.3)", "12.3"},
		{"abs(-1.23)", "1.23"},

		{"null || null", "''"},
		{"'abc' || null", "'abc'"},
		{"null || 'def'", "'def'"},
		{"123 || 'abc'", "'123abc'"},
		{"'abc' || 123", "'abc123'"},
		{"true || 'abc'", "'trueabc'"},
		{"'abc' || false", "'abcfalse'"},
		{"123.456 || 'abc'", "'123.456abc'"},
		{"'abc' || 123.456 || 'abc'", "'abc123.456abc'"},
		{"concat(12, 3.4, null, '56', true)", "'123.456true'"},
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
		v, err := r.Eval(nil)
		if err != nil {
			t.Errorf("Eval(%q) failed with %s", c.s, err)
		}
		if sql.Format(v) != c.r {
			t.Errorf("Eval(%q) got %s want %s", c.s, sql.Format(v), c.r)
		}
	}
	/*
		fail := []string{
			"abc()",
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
	*/
}
