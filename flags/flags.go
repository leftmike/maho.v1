package flags

import (
	"strings"

	"github.com/leftmike/maho/config"
)

type Flag int

const (
	PushdownWhere Flag = iota
)

type flagDefault struct {
	flag Flag
	def  bool
}

var (
	defaultFlags = map[string]flagDefault{
		"pushdown_where": {PushdownWhere, true},
	}
)

func LookupFlag(nam string) (Flag, bool) {
	fd, ok := defaultFlags[strings.ToLower(nam)]
	return fd.flag, ok
}

func ListFlags(fn func(nam string, f Flag)) {
	for nam, fd := range defaultFlags {
		fn(nam, fd.flag)
	}
}

type Flags []bool

func (flgs Flags) GetFlag(f Flag) bool {
	return flgs[f]
}

func Config(cfg *config.Config) Flags {
	flgs := make([]bool, len(defaultFlags))
	for nam, fd := range defaultFlags {
		flgs[fd.flag] = fd.def
		cfg.Var(&flgs[fd.flag], nam).Hide()
	}
	return flgs
}

func Default() Flags {
	flgs := make([]bool, len(defaultFlags))
	for _, fd := range defaultFlags {
		flgs[fd.flag] = fd.def
	}
	return flgs
}
