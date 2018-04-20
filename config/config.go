package config

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

/*
// comment
/* comment * /

${<variable>} = value
[section] // control which parts of the config file are read
<name> = <value>

<value> = <string> | true | false | <number> | ${<variable>} | <map> | <array>
<map> = { <name> : <value> [,] ... }
<array> = [ <value> [,] ... ]
<string> = "..." | `...`
<number> = ...
*/

type value interface {
	Set(string) error
	String() string
}

type setBy int

const (
	byDefault setBy = iota
	byFlag
	byEnv
	byConfig
)

func (b setBy) String() string {
	switch b {
	case byDefault:
		return "default"
	case byFlag:
		return "flag"
	case byEnv:
		return "environment"
	case byConfig:
		return "config-file"
	}
	panic(fmt.Sprintf("set-by: unexpected value: %d", b))
}

type Variable struct {
	cfg  *Config
	name string
	val  value
	flag string
	env  []string
	by   setBy
}

var cfg = NewConfig(flag.CommandLine)

type Config struct {
	vars    map[string]*Variable
	flags   map[string]*Variable
	flagSet *flag.FlagSet
}

func NewConfig(fs *flag.FlagSet) *Config {
	return &Config{
		vars:    map[string]*Variable{},
		flags:   map[string]*Variable{},
		flagSet: fs,
	}
}

func (c *Config) Env() error {
	if !c.flagSet.Parsed() {
		panic("flags must be parsed before the environment")
	}
	c.flagVars()

	for _, v := range c.vars {
		if v.by == byDefault && v.env != nil {
			for _, e := range v.env {
				if s, ok := os.LookupEnv(e); ok {
					err := v.val.Set(s)
					if err != nil {
						return fmt.Errorf("config: %s environment variable: %s", e, err)
					}
					v.by = byEnv
				}
			}
		}
	}

	return nil
}

func Env() error {
	return cfg.Env()
}

func (c *Config) flagVars() {
	c.flagSet.Visit(func(f *flag.Flag) {
		if v, ok := c.flags[f.Name]; ok {
			v.by = byFlag
		}
	})
}

func (c *Config) Load(filename string, reader io.Reader) error {
	if !c.flagSet.Parsed() {
		panic("flags must be parsed before config is loaded")
	}
	c.flagVars()

	// XXX: check file extension to decide how to load the config file
	return nil
}

func Load(filename string, reader io.Reader) error {
	return cfg.Load(filename, reader)
}

func (c *Config) Vars() []*Variable {
	var vars []*Variable
	for _, v := range c.vars {
		vars = append(vars, v)
	}
	return vars
}

func Vars() []*Variable {
	return cfg.Vars()
}

func (c *Config) Var(val interface{}, name string) *Variable {
	if _, ok := c.vars[name]; ok {
		panic(fmt.Sprintf("same config variable, %s, defined twice", name))
	}

	var v value
	if vi, ok := val.(value); ok {
		v = vi
	} else if b, ok := val.(*bool); ok {
		v = (*boolValue)(b)
	} else if d, ok := val.(*time.Duration); ok {
		v = (*durationValue)(d)
	} else if f, ok := val.(*float64); ok {
		v = (*float64Value)(f)
	} else if i, ok := val.(*int); ok {
		v = (*intValue)(i)
	} else if i64, ok := val.(*int64); ok {
		v = (*int64Value)(i64)
	} else if s, ok := val.(*string); ok {
		v = (*stringValue)(s)
	} else if u, ok := val.(*uint); ok {
		v = (*uintValue)(u)
	} else if u64, ok := val.(*uint64); ok {
		v = (*uint64Value)(u64)
	} else {
		// XXX: handle structs and slices

		panic(fmt.Sprintf("can't use %T as a config variable", val))
	}

	nv := &Variable{
		cfg:  c,
		name: name,
		val:  v,
	}
	c.vars[name] = nv
	return nv
}

func Var(val interface{}, name string) *Variable {
	return cfg.Var(val, name)
}

func (v *Variable) Name() string {
	return v.name
}

func (v *Variable) Val() string {
	return v.val.String()
}

func (v *Variable) By() string {
	return v.by.String()
}

func (v *Variable) Flag(name, usage string) *Variable {
	v.flag = name
	v.cfg.flags[v.flag] = v
	v.cfg.flagSet.Var(v.val, v.flag, usage)
	return v
}

func (v *Variable) Usage(usage string) *Variable {
	return v.Flag(v.name, usage)
}

func (v *Variable) Env(vars ...string) *Variable {
	v.env = vars
	return v
}

func (v *Variable) Bool(def bool) *bool {
	b, ok := v.val.(*boolValue)
	if !ok {
		panic(fmt.Sprintf("can't convert %T to bool", v.val))
	}
	*b = (boolValue)(def)
	return (*bool)(b)
}

func (v *Variable) Duration(def time.Duration) *time.Duration {
	d, ok := v.val.(*durationValue)
	if !ok {
		panic(fmt.Sprintf("can't convert %T to duration", v.val))
	}
	*d = (durationValue)(def)
	return (*time.Duration)(d)
}

func (v *Variable) Float64(def float64) *float64 {
	f, ok := v.val.(*float64Value)
	if !ok {
		panic(fmt.Sprintf("can't convert %T to float64", v.val))
	}
	*f = (float64Value)(def)
	return (*float64)(f)
}

func (v *Variable) Int(def int) *int {
	i, ok := v.val.(*intValue)
	if !ok {
		panic(fmt.Sprintf("can't convert %T to int", v.val))
	}
	*i = (intValue)(def)
	return (*int)(i)
}

func (v *Variable) Int64(def int64) *int64 {
	i64, ok := v.val.(*int64Value)
	if !ok {
		panic(fmt.Sprintf("can't convert %T to int64", v.val))
	}
	*i64 = (int64Value)(def)
	return (*int64)(i64)
}

func (v *Variable) String(def string) *string {
	s, ok := v.val.(*stringValue)
	if !ok {
		panic(fmt.Sprintf("can't convert %T to string", v.val))
	}
	*s = (stringValue)(def)
	return (*string)(s)
}

func (v *Variable) Uint(def uint) *uint {
	u, ok := v.val.(*uintValue)
	if !ok {
		panic(fmt.Sprintf("can't convert %T to uint", v.val))
	}
	*u = (uintValue)(def)
	return (*uint)(u)
}

func (v *Variable) Uint64(def uint64) *uint64 {
	u64, ok := v.val.(*uint64Value)
	if !ok {
		panic(fmt.Sprintf("can't convert %T to uint64", v.val))
	}
	*u64 = (uint64Value)(def)
	return (*uint64)(u64)
}
