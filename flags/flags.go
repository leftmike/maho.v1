package flags

import (
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
		"pushdown-where": {PushdownWhere, true},
	}
)

type Flags []bool

func (flgs Flags) GetFlag(f Flag) bool {
	return flgs[f]
}

func Config(cfg *config.Config) Flags {
	flgs := make([]bool, len(defaultFlags))
	for nam, fd := range defaultFlags {
		flgs[fd.flag] = fd.def
		cfg.Var(&flgs[fd.flag], nam)
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
