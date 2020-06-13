package copy

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/leftmike/maho/sql"
)

func CopyFromText(rdr *Reader, numCols int, delim rune, fn func(vals []sql.Value) error) error {
	vals := make([]sql.Value, numCols)
	for {
		for cdx := 0; cdx < numCols; cdx += 1 {
			s, null, err := readTextColumn(rdr, delim, cdx == numCols-1)
			if err == io.EOF && cdx == 0 {
				return nil
			} else if err != nil {
				return fmt.Errorf("engine: %s: %s", rdr.Position(), err)
			}
			if null {
				if s != "" {
					return fmt.Errorf(`engine: %s: null (\N) must be alone in column`,
						rdr.Position())
				}
				vals[cdx] = nil
			} else {
				vals[cdx] = sql.StringValue(s)
			}
		}

		err := fn(vals)
		if err != nil {
			return fmt.Errorf("engine: %s: %s", rdr.Position(), err)
		}
	}
}

func readTextColumn(rdr *Reader, delim rune, last bool) (string, bool, error) {
	var buf strings.Builder
	var null bool

	for {
		r, err := rdr.ReadRune()
		if err != nil {
			return "", false, err
		}
		if r == delim {
			if last {
				return "", false, errors.New("unexpected delimiter for last column")
			}
			break
		} else if r == '\n' {
			if !last {
				return "", false, errors.New("unexpected end of line")
			}
			break
		} else if r == '\\' {
			r, err = rdr.ReadRune()
			if err != nil {
				return "", false, err
			}
			if r == 'N' {
				null = true
			} else {
				switch r {
				case 'b':
					r = 8
				case 'f':
					r = 12
				case 'n':
					r = 10
				case 'r':
					r = 13
				case 't':
					r = 9
				case 'v':
					r = 11
				}
				buf.WriteRune(r)
			}
		} else {
			buf.WriteRune(r)
		}
	}

	return buf.String(), null, nil
}
