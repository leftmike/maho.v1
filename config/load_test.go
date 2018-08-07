package config

import (
	"flag"
	"reflect"
	"strings"
	"testing"
)

func newConfig(bv bool, i64v int64, sv string) (*Config, *bool, *int64, *string) {
	c := NewConfig(flag.NewFlagSet("test", flag.PanicOnError))
	c.Var(new(string), "good")
	b := c.Var(new(bool), "bool_var").Bool(bv)
	i64 := c.Var(new(int64), "int64_var").Int64(i64v)
	s := c.Var(new(string), "string-var").String(sv)
	return c, b, i64, s
}

func TestLoadSimple(t *testing.T) {
	cases := []struct {
		bv, be     bool
		i64v, i64e int64
		sv, se     string
		fail       bool
		cfg        string
	}{
		{fail: true, cfg: `good`},
		{fail: true, cfg: `good=`},
		{fail: true, cfg: `good =`},
		{fail: true, cfg: `bad = 123`},
		{cfg: `good=123`},
		{cfg: `/* comment */ good // comment
= /* comment */

        123`},
		{fail: true, cfg: `1234`},
		{fail: true, cfg: `"bad" = 1234`},
		{cfg: `"good" = 1234`},

		{bv: false, be: true, cfg: `bool_var = true`},
		{bv: true, be: false, cfg: `bool_var = false`},
		{fail: true, cfg: `bool_var = 1234`},
		{i64v: 1234, i64e: -5678, cfg: `int64_var = -5678`},
		{fail: true, cfg: `int64_var = "a string"`},
		{sv: "", se: "a string", cfg: "string-var = `a string`"},
		{sv: "", se: "a string", cfg: `string-var = "a string"`},
		{fail: true, cfg: `bool_var = {a: 10 b: 20, c:30}`},
	}

	for i, tc := range cases {
		c, b, i64, s := newConfig(tc.bv, tc.i64v, tc.sv)
		if *b != tc.bv || *i64 != tc.i64v || *s != tc.sv {
			t.Errorf("NewConfig(%d) defaults not correctly set", i)
		}
		err := c.load(strings.NewReader(tc.cfg))
		if tc.fail {
			if err == nil {
				t.Errorf("load(%q) did not fail", tc.cfg)
			}
		} else {
			if err != nil {
				t.Errorf("load(%q) failed with %s", tc.cfg, err)
			} else if *b != tc.be || *i64 != tc.i64e || *s != tc.se {
				t.Errorf("load(%q) variables not updated correctly", tc.cfg)
			}
		}
	}
}

func TestLoadArray(t *testing.T) {
	cases := []struct {
		a    []interface{}
		fail bool
		cfg  string
	}{
		{fail: true, cfg: `array=[`},
		{fail: true, cfg: `array=[,]`},
		{fail: true, cfg: `array=,]`},
		{fail: true, cfg: `array=]`},
		{fail: true, cfg: `array=[abc,, def]`},
		{a: []interface{}{"abc"}, cfg: `array=["abc"]`},
		{a: []interface{}{"abc", "def", "ghi"}, cfg: `array=[abc, def ghi]`},
	}

	for _, tc := range cases {
		c := NewConfig(flag.NewFlagSet("test", flag.PanicOnError))
		a := c.Var(new(Array), "array").Array()
		err := c.load(strings.NewReader(tc.cfg))
		if tc.fail {
			if err == nil {
				t.Errorf("load(%q) did not fail", tc.cfg)
			}
		} else {
			if err != nil {
				t.Errorf("load(%q) failed with %s", tc.cfg, err)
			} else if reflect.DeepEqual(tc.a, *a) {
				t.Errorf("load(%q) variables not updated correctly", tc.cfg)
			}
		}
	}
}

func TestLoadMap(t *testing.T) {
	cases := []struct {
		m    Map
		fail bool
		cfg  string
	}{
		{fail: true, cfg: `map={`},
		{fail: true, cfg: `map={,}`},
		{fail: true, cfg: `map=,}`},
		{fail: true, cfg: `map=}`},
		{fail: true, cfg: `map={abc: 1,, def: 2}`},
		{fail: true, cfg: `map={abc 1 def 2}`},
		{m: Map{"abc": 1, "def": 2, "ghi": 3}, cfg: `map={abc:1,"def":2 ghi : 3}`},
	}

	for _, tc := range cases {
		c := NewConfig(flag.NewFlagSet("test", flag.PanicOnError))
		m := c.Var(make(Map), "map").Map()
		err := c.load(strings.NewReader(tc.cfg))
		if tc.fail {
			if err == nil {
				t.Errorf("load(%q) did not fail", tc.cfg)
			}
		} else {
			if err != nil {
				t.Errorf("load(%q) failed with %s", tc.cfg, err)
			} else if reflect.DeepEqual(tc.m, m) {
				t.Errorf("load(%q) variables not updated correctly", tc.cfg)
			}
		}
	}
}
