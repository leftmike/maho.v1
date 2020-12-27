package sql

import (
	"fmt"
	"runtime"
)

const (
	MajorVersion = 0
	MinorVersion = 1
)

func Version() string {
	// PostgreSQL 10.12 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 4.9.3, 64-bit
	return fmt.Sprintf("Maho %d.%d on %s %s, compiled by %s", MajorVersion, MinorVersion,
		runtime.GOARCH, runtime.GOOS, runtime.Version())
}
