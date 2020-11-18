package repl

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/peterh/liner"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/parser"
)

const (
	mahoHistory = ".maho_history"
)

type lineReader struct {
	line *liner.State
	r    *strings.Reader
}

func (lr *lineReader) ReadRune() (r rune, size int, err error) {
	for {
		if lr.r == nil {
			s, err := lr.line.Prompt("maho: ")
			if err != nil {
				return 0, 0, err
			}
			lr.line.AppendHistory(s)
			lr.r = strings.NewReader(s)
		}

		r, sz, err := lr.r.ReadRune()
		if err == io.EOF {
			lr.r = nil
		} else if err != nil {
			return 0, 0, err
		} else {
			return r, sz, nil
		}
	}
}

func Interact() evaluate.SessionHandler {
	line := liner.NewLiner()
	defer line.Close()

	if f, err := os.Open(mahoHistory); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	return func(ses *evaluate.Session) {
		ReplSQL(ses, parser.NewParser(&lineReader{line: line}, "console"), os.Stdout)

		if f, err := os.Create(mahoHistory); err != nil {
			fmt.Fprintf(os.Stderr, "maho: error writing history file, %s: %s", mahoHistory, err)
		} else {
			line.WriteHistory(f)
			f.Close()
		}
	}
}
