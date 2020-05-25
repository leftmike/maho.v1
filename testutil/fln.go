package testutil

import (
	"fmt"
	"path/filepath"
	"runtime"
)

type FileLineNumber struct {
	File string
	Line int
}

func (fln FileLineNumber) String() string {
	if fln.File == "" || fln.Line == 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d: ", filepath.Base(fln.File), fln.Line)
}

func MakeFileLineNumber() FileLineNumber {
	_, fn, ln, ok := runtime.Caller(2)
	if !ok {
		return FileLineNumber{}
	}
	return FileLineNumber{fn, ln}
}
