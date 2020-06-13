package copy

import (
	"fmt"
	"io"
)

type Reader struct {
	rr         io.RuneReader
	eof        bool
	unread     bool
	unreadRune rune
	filename   string
	line       int
	column     int
}

func NewReader(filename string, rr io.RuneReader, line int) *Reader {
	return &Reader{
		rr:       rr,
		filename: filename,
		line:     line,
		column:   0,
	}
}

func (rdr *Reader) Position() string {
	return fmt.Sprintf("%s:%d:%d", rdr.filename, rdr.line, rdr.column)
}

func (rdr *Reader) ReadRune() (rune, error) {
	if rdr.eof {
		return 0, io.EOF
	}

	var r rune
	if rdr.unread {
		rdr.unread = false
		r = rdr.unreadRune
	} else {
		var err error
		r, _, err = rdr.rr.ReadRune()
		if err != nil {
			return 0, err
		}
	}

	if r == '\n' {
		rdr.line += 1
		rdr.column = 0
	} else {
		rdr.column += 1
	}

	if r == '\\' {
		var err error
		rdr.unreadRune, _, err = rdr.rr.ReadRune()
		if err == io.EOF {
			rdr.eof = true
		} else if err != nil {
			return 0, err
		}
		if rdr.unreadRune == '.' && rdr.column == 1 {
			rdr.eof = true
			return 0, io.EOF
		} else {
			rdr.unread = true
		}
	}

	return r, nil
}
