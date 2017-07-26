package engine_test

import (
	"fmt"
	. "maho/engine"
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
		{"1 + null", sql.NullString},
		{"null + 2.3", sql.NullString},
		{"123 + 456", "579"},
		{"123 + 4.56", fmt.Sprintf("%v", 123+4.56)},
		{"12.3 + 456", fmt.Sprintf("%v", 12.3+456)},
		{"1.23 + 45.6", fmt.Sprintf("%v", 1.23+45.6)},

		{"1 / null", sql.NullString},
		{"null / 2.3", sql.NullString},
		{"456 / 123", "3"},
		{"123 / 45.6", fmt.Sprintf("%v", 123/45.6)},
		{"123.45 / 6", fmt.Sprintf("%v", 123.45/6)},
		{"12.3 / 45.6", fmt.Sprintf("%v", 12.3/45.6)},

		{"1 * null", sql.NullString},
		{"null * 2.3", sql.NullString},
		{"456 * 123", "56088"},
		{"123 * 45.6", fmt.Sprintf("%v", 123*45.6)},
		{"123.45 * 6", fmt.Sprintf("%v", 123.45*6)},
		{"12.3 * 45.6", fmt.Sprintf("%v", 12.3*45.6)},

		{"- null", sql.NullString},
		{"- (1 + null)", sql.NullString},
		{"- 123", "-123"},
		{"- 123.456", "-123.456"},
		{"- (123 + 456)", "-579"},
		{"- (1.23 + 4.5)", "-5.73"},

		{"1 - null", sql.NullString},
		{"null - 2.3", sql.NullString},
		{"456 - 123", "333"},
		{"123 - 45.6", fmt.Sprintf("%v", 123-45.6)},
		{"123.45 - 6", fmt.Sprintf("%v", 123.45-6)},
		{"12.3 - 45.6", fmt.Sprintf("%v", 12.3-45.6)},

		{"1 & null", sql.NullString},
		{"null & 2", sql.NullString},
		{"15 & 2", "2"},
		{"12345 & 67890", fmt.Sprintf("%v", 12345&67890)},

		{"1 << null", sql.NullString},
		{"null << 2", sql.NullString},
		{"1 << 2", "4"},
		{"123 << 4", fmt.Sprintf("%v", 123<<4)},

		{"1 % null", sql.NullString},
		{"null % 2", sql.NullString},
		{"15 % 4", "3"},
		{"12345 % 67", fmt.Sprintf("%v", 12345%67)},

		{"1 | null", sql.NullString},
		{"null | 2", sql.NullString},
		{"1 | 2", "3"},
		{"12345 | 67890", fmt.Sprintf("%v", 12345|67890)},

		{"1 >> null", sql.NullString},
		{"null >> 2", sql.NullString},
		{"16 >> 2", "4"},
		{"123456789 >> 4", fmt.Sprintf("%v", 123456789>>4)},

		{"null AND true", sql.NullString},
		{"false AND null", sql.NullString},
		{"false AND true", sql.FalseString},
		{"true AND false", sql.FalseString},
		{"true AND true", sql.TrueString},
		{"false AND false", sql.FalseString},

		{"null OR true", sql.NullString},
		{"false OR null", sql.NullString},
		{"false OR true", sql.TrueString},
		{"true OR false", sql.TrueString},
		{"true OR true", sql.TrueString},
		{"false OR false", sql.FalseString},

		{"NOT null", sql.NullString},
		{"NOT false", sql.TrueString},
		{"NOT true", sql.FalseString},

		{"abs(null)", sql.NullString},
		{"abs(123)", "123"},
		{"abs(-123)", "123"},
		{"abs(12.3)", "12.3"},
		{"abs(-1.23)", "1.23"},

		{"null || null", "''"},
		{"'abc' || null", "'abc'"},
		{"null || 'def'", "'def'"},
		{"123 || 'abc'", "'123abc'"},
		{"'abc' || 123", "'abc123'"},
		{"true || 'abc'", "'" + sql.TrueString + "abc'"},
		{"'abc' || false", "'abc" + sql.FalseString + "'"},
		{"123.456 || 'abc'", "'123.456abc'"},
		{"'abc' || 123.456 || 'abc'", "'abc123.456abc'"},
		{"concat(12, 3.4, null, '56', true)", "'123.456" + sql.TrueString + "'"},

		{"true == false", sql.FalseString},
		{"true == true", sql.TrueString},
		{"false == false", sql.TrueString},
		{"true == null", sql.NullString},
		{"null == false", sql.NullString},
		{"null == null", sql.NullString},
		{"true != false", sql.TrueString},
		{"true != true", sql.FalseString},
		{"false != false", sql.FalseString},
		{"true != null", sql.NullString},
		{"null != false", sql.NullString},
		{"null != null", sql.NullString},

		{"null == 123", sql.NullString},
		{"12.3 == null", sql.NullString},
		{"null >= 123", sql.NullString},
		{"12.3 >= null", sql.NullString},
		{"null > 123", sql.NullString},
		{"12.3 > null", sql.NullString},
		{"null <= 123", sql.NullString},
		{"12.3 <= null", sql.NullString},
		{"null < 123", sql.NullString},
		{"12.3 < null", sql.NullString},
		{"null != 123", sql.NullString},
		{"12.3 != null", sql.NullString},

		{"null == 'abc'", sql.NullString},
		{"'abcd' == null", sql.NullString},
		{"null >= 'abc'", sql.NullString},
		{"'abcd' >= null", sql.NullString},
		{"null > 'abc'", sql.NullString},
		{"'abcd' > null", sql.NullString},
		{"null <= 'abc'", sql.NullString},
		{"'abcd' <= null", sql.NullString},
		{"null < 'abc'", sql.NullString},
		{"'abcd' < null", sql.NullString},
		{"null != 'abc'", sql.NullString},
		{"'abcd' != null", sql.NullString},
	}

	for i, c := range cases {
		var p parser.Parser
		p.Init(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i))
		e, err := p.ParseExpr()
		if err != nil {
			t.Errorf("ParseExpr(%q) failed with %s", c.s, err)
			continue
		}
		r, err := Compile(nil, e)
		if err != nil {
			t.Errorf("Compile(%q) failed with %s", c.s, err)
			continue
		}
		v, err := r.Eval(nil)
		if err != nil {
			t.Errorf("Eval(%q) failed with %s", c.s, err)
			continue
		}
		if sql.Format(v) != c.r {
			t.Errorf("Eval(%q) got %s want %s", c.s, sql.Format(v), c.r)
		}
	}

	numberCases := []string{
		"-123.4",
		"-123",
		"123",
		"123.4",
		"124",
		"456",
		"456.7",
	}

	for i, m := range numberCases {
		for j, n := range numberCases {
			numberTest(t, m, "==", n, i == j)
			numberTest(t, m, ">=", n, i >= j)
			numberTest(t, m, ">", n, i > j)
			numberTest(t, m, "<=", n, i <= j)
			numberTest(t, m, "<", n, i < j)
			numberTest(t, m, "!=", n, i != j)
		}
	}

	stringCases := []string{
		"'ABC'",
		"'abc'",
		"'abcA'",
		"'abca'",
		"'abcd'",
		"'abcde'",
		"'bcde'",
	}

	for i, m := range stringCases {
		for j, n := range stringCases {
			compareTest(t, m, "==", n, i == j)
			compareTest(t, m, ">=", n, i >= j)
			compareTest(t, m, ">", n, i > j)
			compareTest(t, m, "<=", n, i <= j)
			compareTest(t, m, "<", n, i < j)
			compareTest(t, m, "!=", n, i != j)
		}
	}

	fail := []string{
		"123 + 'abc'",
		"'abc' + 12.34",
		"true + 123",
		"123 / 'abc'",
		"'abc' / 12.34",
		"true / 123",
		"123 % 'abc'",
		"'abc' % 12.34",
		"true % 123",
		"123 * 'abc'",
		"'abc' * 12.34",
		"true * 123",
		"123 - 'abc'",
		"'abc' - 12.34",
		"true - 123",

		"123 AND true",
		"'abc' AND false",
		"true AND 12.34",
		"123 OR true",
		"'abc' OR false",
		"true OR 12.34",

		"123 & true",
		"123 & 'abc'",
		"12.34 & 567",
		"true | 123",
		"'abc' | 123",
		"123 | 45.67",

		"123 == 'abc'",
		"'abc' == true",
		"12.34 == false",
		"123 <= 'abc'",
		"'abc' <= true",
		"12.34 <= false",
		"123 < 'abc'",
		"'abc' < true",
		"12.34 < false",
		"123 >= 'abc'",
		"'abc' >= true",
		"12.34 >= false",
		"123 > 'abc'",
		"'abc' > true",
		"12.34 > false",
		"123 != 'abc'",
		"'abc' != true",
		"12.34 != false",

		"'abc' << 12",
		"12 << true",
		"12 << -34",
		"12 << 3.4",
		"'abc' >> 12",
		"12 >> true",
		"12 >> -34",
		"12 >> 3.4",

		"- true",
		"- 'abc'",

		"not 'abc'",
		"not 123",
		"not 12.34",

		"abs(true)",
		"abs('xyz')",
	}

	for i, f := range fail {
		var p parser.Parser
		p.Init(strings.NewReader(f), fmt.Sprintf("fail[%d]", i))
		e, err := p.ParseExpr()
		if err != nil {
			t.Errorf("ParseExpr(%q) failed with %s", f, err)
			continue
		}
		r, err := Compile(nil, e)
		if err != nil {
			t.Errorf("Compile(%q) failed with %s", f, err)
			continue
		}
		v, err := r.Eval(nil)
		if err == nil {
			t.Errorf("Eval(%q) did not fail, got %s", f, sql.Format(v))
		}
	}
}

func numberTest(t *testing.T, m, op, n string, b bool) {
	compareTest(t, m, op, n, b)
	if !strings.ContainsRune(m, '.') {
		compareTest(t, m+".0", op, n, b)
	}
	if !strings.ContainsRune(n, '.') {
		compareTest(t, m, op, n+".0", b)
	}
}

func compareTest(t *testing.T, m, op, n string, b bool) {
	s := m + op + n
	var p parser.Parser
	p.Init(strings.NewReader(s), s)
	e, err := p.ParseExpr()
	if err != nil {
		t.Errorf("ParseExpr(%q) failed with %s", s, err)
		return
	}
	r, err := Compile(nil, e)
	if err != nil {
		t.Errorf("Compile(%q) failed with %s", s, err)
		return
	}
	v, err := r.Eval(nil)
	if err != nil {
		t.Errorf("Eval(%q) failed with %s", s, err)
		return
	}
	var ret string
	if b {
		ret = sql.TrueString
	} else {
		ret = sql.FalseString
	}
	if sql.Format(v) != ret {
		t.Errorf("Eval(%q) got %s want %s", s, sql.Format(v), ret)
	}
}
