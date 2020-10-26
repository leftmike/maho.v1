package config_test

import (
	"flag"
	"os"
	"reflect"
	"testing"

	"github.com/leftmike/maho/config"
)

func TestFlags(t *testing.T) {
	fs := flag.NewFlagSet("test_flags", flag.ContinueOnError)
	cfg := config.NewConfig(fs)
	b := cfg.Var(new(bool), "bool").Usage("bool variable").Bool(true)
	i := cfg.Var(new(int), "int").Usage("int variable").Int(123)
	s := cfg.Var(new(string), "string").String("default")
	if *b != true {
		t.Errorf("*b != true")
	}
	if *i != 123 {
		t.Errorf("*i != 123")
	}
	if *s != "default" {
		t.Errorf("*s != \"default\"")
	}
	err := fs.Parse([]string{"-bool=false", "-int", "456"})
	if err != nil {
		t.Fatalf("fs.Parse() failed with %s", err)
	}
	if *b != false {
		t.Errorf("*b != false")
	}
	if *i != 456 {
		t.Errorf("*i != 456")
	}
	if *s != "default" {
		t.Errorf("*s != \"default\"")
	}
}

func TestEnv(t *testing.T) {
	fs := flag.NewFlagSet("test_flags", flag.ContinueOnError)
	cfg := config.NewConfig(fs)
	b := cfg.Var(new(bool), "bool").Env("X-BOOL").Usage("bool variable").Bool(true)
	i := cfg.Var(new(int), "int").Usage("int variable").Env("X-INT").Int(123)
	s := cfg.Var(new(string), "string").Usage("string variable").Env("X-STRING").String("default")
	if *b != true {
		t.Errorf("*b != true")
	}
	if *i != 123 {
		t.Errorf("*i != 123")
	}
	if *s != "default" {
		t.Errorf("*s != \"default\"")
	}
	err := os.Setenv("X-BOOL", "true")
	if err != nil {
		t.Fatalf("os.Setenv() failed with %s", err)
	}
	err = os.Setenv("X-STRING", "from environment")
	if err != nil {
		t.Fatalf("os.Setenv() failed with %s", err)
	}
	err = fs.Parse([]string{"-bool=false", "-int", "456"})
	if err != nil {
		t.Fatalf("fs.Parse() failed with %s", err)
	}
	err = cfg.Env()
	if err != nil {
		t.Errorf("cfg.Env() failed with %s", err)
	}
	if *b != false {
		t.Errorf("*b != false")
	}
	if *i != 456 {
		t.Errorf("*i != 456")
	}
	if *s != "from environment" {
		t.Errorf("*s != \"from environment\"")
	}
}

func TestArray(t *testing.T) {
	fs := flag.NewFlagSet("test_flags", flag.ContinueOnError)
	cfg := config.NewConfig(fs)
	a := cfg.Var(new(config.Array), "array").Usage("array variable").Array()
	err := fs.Parse([]string{"-array=abc", "-array=def"})
	if err != nil {
		t.Fatalf("fs.Parse() failed with %s", err)
	}
	if !reflect.DeepEqual(*a, config.Array{"abc", "def"}) {
		t.Errorf("*a != Array{\"abc\", \"def\"}")
	}
}

func TestMapFlag(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("c.Var(make(config.Map)).Flag() did not fail")
		}
	}()

	c := config.NewConfig(flag.NewFlagSet("test", flag.ContinueOnError))
	c.Var(make(config.Map), "map").Usage("map")
}

func TestMapEnv(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("c.Var(make(config.Map)).Env() did not fail")
		}
	}()

	c := config.NewConfig(flag.NewFlagSet("test", flag.ContinueOnError))
	c.Var(make(config.Map), "map").Env("X-MAP")
}
