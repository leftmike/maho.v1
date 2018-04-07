package config

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/scanner"
	"time"
	"unicode"
)

type Value interface {
	Set(string) error
	String() string
}

type Option int

const (
	Default      Option = 0
	NoUpdate     Option = 1 << iota // can not be updated after startup
	NoConfigFile                    // can not be specified in a config file
)

func addOption(s, opt string) string {
	if s != "" {
		s += " | "
	}
	return s + opt
}

func (o Option) String() string {
	var s string
	if (o & NoUpdate) != 0 {
		s = addOption(s, "NoUpdate")
	}
	if (o & NoConfigFile) != 0 {
		s = addOption(s, "NoConfigFile")
	}
	if s == "" {
		return "Default"
	}
	return s
}

type Param struct {
	Name    string
	Val     Value
	Options Option
}

type nameVal struct {
	name string
	val  string
}

type config struct {
	params     map[string]*Param
	args       []nameVal
	configFile string
	noConfig   bool
	list       bool
}

var cfg = &config{}

func (cfg *config) Set(s string) error {
	ss := strings.SplitN(s, "=", 2)
	if len(ss) != 2 {
		return fmt.Errorf("config: expected name=value; got %s", s)
	}
	cfg.args = append(cfg.args, nameVal{ss[0], ss[1]})
	return nil
}

func (_ *config) String() string {
	return ""
}

func (cfg *config) flags(fs *flag.FlagSet, param, noConfig, configFile, listConfig string) {
	fs.Var(cfg, param, "set `param=value`")

	if noConfig != "" {
		fs.BoolVar(&cfg.noConfig, noConfig, false, "don't load a config file")
	}
	if configFile != "" {
		fs.StringVar(&cfg.configFile, configFile, "", "`file` to load config from")
	}
	if listConfig != "" {
		fs.BoolVar(&cfg.list, listConfig, false, "list the config and then exit")
	}
}

func Flags(param, noConfig, configFile, listConfig string) {
	cfg.flags(flag.CommandLine, param, noConfig, configFile, listConfig)
}

type paramSlice []*Param

func (ps paramSlice) Len() int {
	return len(ps)
}

func (ps paramSlice) Swap(i, j int) {
	ps[i], ps[j] = ps[j], ps[i]
}

func (ps paramSlice) Less(i, j int) bool {
	return strings.Compare(ps[i].Name, ps[j].Name) < 0
}

func (cfg *config) allParams() []*Param {
	list := make([]*Param, 0, len(cfg.params))
	for _, param := range cfg.params {
		list = append(list, param)
	}
	sort.Sort(paramSlice(list))
	return list
}

func AllParams() []*Param {
	return cfg.allParams()
}

func (cfg *config) listConfig() {
	for _, param := range cfg.allParams() {
		fmt.Printf("%s=%s\n", param.Name, param.Val)
	}
}

const (
	lineWhitespace   = (1 << ' ') | (1 << '\t') | (1 << '\n') | (1 << '\r')
	noLineWhitespace = (1 << ' ') | (1 << '\t')
)

func (cfg *config) loadConfig(configFile string) error {
	f, err := os.Open(configFile)
	if err != nil {
		return err
	}
	defer f.Close()
	s := scanner.Scanner{
		Mode: scanner.ScanIdents | scanner.ScanFloats | scanner.ScanStrings |
			scanner.ScanRawStrings | scanner.ScanComments | scanner.SkipComments,
		Whitespace: lineWhitespace,
		IsIdentRune: func(r rune, i int) bool {
			if i == 0 {
				return unicode.IsLetter(r)
			}
			return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_'
		},
	}
	s.Init(bufio.NewReader(f))

	for {
		s.Whitespace = lineWhitespace
		tok := s.Scan()
		if tok == scanner.EOF {
			break
		}
		if tok != scanner.Ident && tok != scanner.String {
			return fmt.Errorf("%s: expected a parameter", s.Pos())
		}
		name := s.TokenText()

		s.Whitespace = noLineWhitespace
		tok = s.Scan()
		if tok != '=' {
			return fmt.Errorf("%s: expected '='", s.Pos())
		}
		tok = s.Scan()
		val := s.TokenText()
		switch tok {
		case scanner.Ident:
		case scanner.Int:
		case scanner.Float:
		case scanner.String:
			val = strings.Trim(val, "\"`")
		case '-':
			tok = s.Scan()
			if tok != scanner.Int && tok != scanner.Float {
				return fmt.Errorf("%s: expected a value", s.Pos())
			}
			val = "-" + s.TokenText()
		default:
			return fmt.Errorf("%s: expected a value", s.Pos())
		}
		err := cfg.setParam(name, val, NoConfigFile)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cfg *config) setParam(name, val string, opt Option) error {
	param, ok := cfg.params[name]
	if !ok {
		return fmt.Errorf("%s is not a param", name)
	}
	if (param.Options & opt) != 0 {
		if opt == NoUpdate {
			return fmt.Errorf("%s may not be updated", name)
		} else if opt == NoConfigFile {
			return fmt.Errorf("%s may not be set in a config file", name)
		}
		panic("unexpected option")
	}

	err := param.Val.Set(val)
	if err != nil {
		return fmt.Errorf("param %s: %s", name, err)
	}
	return nil
}

func (cfg *config) update(name, val string) error {
	return cfg.setParam(name, val, NoUpdate)
}

func Update(name, val string) error {
	return cfg.update(name, val)
}

func (cfg *config) load(configFile string) error {
	if !cfg.noConfig {
		if cfg.configFile != "" {
			configFile = cfg.configFile
		}
		err := cfg.loadConfig(configFile)
		if err != nil {
			return err
		}
	}

	for _, arg := range cfg.args {
		err := cfg.setParam(arg.name, arg.val, Default)
		if err != nil {
			return err
		}
	}

	if cfg.list {
		cfg.listConfig()
		os.Exit(0)
	}
	return nil
}

func Load(configFile string) error {
	return cfg.load(configFile)
}

func (cfg *config) boolParam(p *bool, name string, b bool, opts Option) *bool {
	*p = b
	cfg.param((*boolValue)(p), name, opts)
	return p
}

func BoolParam(p *bool, name string, b bool, opts Option) *bool {
	return cfg.boolParam(p, name, b, opts)
}

func (cfg *config) durationParam(p *time.Duration, name string, d time.Duration,
	opts Option) *time.Duration {

	*p = d
	cfg.param((*durationValue)(p), name, opts)
	return p
}

func DurationParam(p *time.Duration, name string, d time.Duration, opts Option) *time.Duration {
	return cfg.durationParam(p, name, d, opts)
}

func (cfg *config) float64Param(p *float64, name string, f float64, opts Option) *float64 {
	*p = f
	cfg.param((*float64Value)(p), name, opts)
	return p
}

func Float64Param(p *float64, name string, f float64, opts Option) *float64 {
	return cfg.float64Param(p, name, f, opts)
}

func (cfg *config) intParam(p *int, name string, i int, opts Option) *int {
	*p = i
	cfg.param((*intValue)(p), name, opts)
	return p
}

func IntParam(p *int, name string, i int, opts Option) *int {
	return cfg.intParam(p, name, i, opts)
}

func (cfg *config) int64Param(p *int64, name string, i int64, opts Option) *int64 {
	*p = i
	cfg.param((*int64Value)(p), name, opts)
	return p
}

func Int64Param(p *int64, name string, i int64, opts Option) *int64 {
	return cfg.int64Param(p, name, i, opts)
}

func (cfg *config) stringParam(p *string, name string, s string, opts Option) *string {
	*p = s
	cfg.param((*stringValue)(p), name, opts)
	return p
}

func StringParam(p *string, name string, s string, opts Option) *string {
	return cfg.stringParam(p, name, s, opts)
}

func (cfg *config) uintParam(p *uint, name string, u uint, opts Option) *uint {
	*p = u
	cfg.param((*uintValue)(p), name, opts)
	return p
}

func UintParam(p *uint, name string, u uint, opts Option) *uint {
	return cfg.uintParam(p, name, u, opts)
}

func (cfg *config) uint64Param(p *uint64, name string, u uint64, opts Option) *uint64 {
	*p = u
	cfg.param((*uint64Value)(p), name, opts)
	return p
}

func Uint64Param(p *uint64, name string, u uint64, opts Option) *uint64 {
	return cfg.uint64Param(p, name, u, opts)
}

func (cfg *config) param(val Value, name string, opts Option) {
	if _, ok := cfg.params[name]; ok {
		panic(fmt.Sprintf("config: param redefined: %s", name))
	}
	if cfg.params == nil {
		cfg.params = make(map[string]*Param)
	}
	cfg.params[name] = &Param{name, val, opts}
}

func Parameter(val Value, name string, opts Option) {
	cfg.param(val, name, opts)
}
