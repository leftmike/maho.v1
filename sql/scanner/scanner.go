package scanner

import (
	"bytes"
	"fmt"
	"io"
	"maho/sql"
	"strconv"
	"unicode"
)

const (
	EOF = -(iota + 1)
	Error
	Identifier
	Reserved
	String
	Integer
	Double
)

type Position struct {
	Filename string
	Line     int
	Column   int
}

type Scanner struct {
	rr         io.RuneReader
	unread     bool
	read       rune
	line       int
	column     int
	buffer     bytes.Buffer
	Error      error
	Identifier sql.Identifier // Identifier and Reserved
	String     string
	Integer    int64
	Double     float64
	Position
}

func (pos Position) String() string {
	s := pos.Filename
	if pos.Line > 0 {
		s += fmt.Sprintf(":%d:%d", pos.Line, pos.Column)
	}
	return s
}

func (s *Scanner) Init(rr io.RuneReader, fn string) *Scanner {
	s.rr = rr
	s.Filename = fn
	s.line = 1
	s.column = 0
	return s
}

func (s *Scanner) Scan() rune {
	s.buffer.Reset()

SkipWhitespace:
	r := s.readRune()

	for {
		if r < 0 {
			return r
		}
		if !unicode.IsSpace(r) {
			break
		}

		r = s.readRune()
	}

	if r == '-' {
		if r := s.readRune(); r == '-' {
			for {
				r = s.readRune()
				if r < 0 {
					return r
				}

				if r == '\n' {
					break
				}
			}

			goto SkipWhitespace
		} else if r < 0 {
			if r != EOF {
				return r
			}
		} else {
			s.unreadRune()
		}
	} else if r == '/' {
		if r = s.readRune(); r == '*' {
			var p rune

			for {
				r = s.readRune()
				if r < 0 {
					return r
				}

				if p == '*' && r == '/' {
					break
				}
				p = r
			}

			goto SkipWhitespace
		} else if r < 0 {
			if r != EOF {
				return r
			}
		} else {
			s.unreadRune()
		}
	}

	s.Column = s.column
	s.Line = s.line

	if unicode.IsLetter(r) || r == '_' {
		return s.scanIdentifier(r)
	} else if unicode.IsDigit(r) {
		return s.scanNumber(r, 1)
	} else if r == '+' {
		r = s.readRune()
		if unicode.IsDigit(r) {
			return s.scanNumber(r, 1)
		}
		s.unreadRune()
		return '+'
	} else if r == '-' {
		r = s.readRune()
		if unicode.IsDigit(r) {
			return s.scanNumber(r, -1)
		}
		s.unreadRune()
		return '-'
	} else if r == '"' || r == '`' {
		return s.scanQuotedIdentifier(r)
	} else if r == '[' {
		return s.scanQuotedIdentifier(']')
	} else if r == '\'' {
		return s.scanString()
	} else if r == '.' || r == ',' || r == '(' || r == ')' || r == '*' {
		return r
	}

	s.Error = fmt.Errorf("unexpected character: %c", r)
	return Error
}

func (s *Scanner) readRune() rune {
	if s.unread {
		s.unread = false
		return s.read
	}

	var err error
	s.read, _, err = s.rr.ReadRune()
	if err == io.EOF {
		return EOF
	} else if err != nil {
		s.Error = err
		return Error
	}

	if s.read == '\n' {
		s.line += 1
		s.column = 0
	} else {
		s.column += 1
	}

	return s.read
}

func (s *Scanner) unreadRune() {
	s.unread = true
}

func (s *Scanner) scanIdentifier(r rune) rune {
	for {
		s.buffer.WriteRune(r)
		r = s.readRune()
		if r == EOF {
			break
		} else if r == Error {
			return Error
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '$' {
			s.unreadRune()
			break
		}
	}

	s.Identifier = sql.Id(s.buffer.String())
	if s.Identifier.IsReserved() {
		return Reserved
	}
	return Identifier
}

func (s *Scanner) scanNumber(r rune, sign int64) rune {
	dbl := false
	for {
		s.buffer.WriteRune(r)
		r = s.readRune()
		if r == EOF {
			break
		} else if r == Error {
			return Error
		}
		if !dbl && r == '.' {
			dbl = true
		} else if !unicode.IsDigit(r) {
			s.unreadRune()
			break
		}
	}

	var err error
	if dbl {
		s.Double, err = strconv.ParseFloat(s.buffer.String(), 64)
	} else {
		s.Integer, err = strconv.ParseInt(s.buffer.String(), 10, 64)
	}
	if err != nil {
		s.Error = err
		return Error
	}
	if dbl {
		s.Double *= float64(sign)
		return Double
	} else {
		s.Integer *= sign
		return Integer
	}
}

func (s *Scanner) scanQuotedIdentifier(delim rune) rune {
	for {
		r := s.readRune()
		if r == EOF {
			s.Error = fmt.Errorf("quoted identifier missing terminating '%c'", delim)
			return Error
		}
		if r == Error {
			return Error
		}
		if r == delim {
			break
		}
		s.buffer.WriteRune(r)
	}

	s.Identifier = sql.QuotedId(s.buffer.String())
	return Identifier
}

func (s *Scanner) scanString() rune {
	for {
		r := s.readRune()
		if r == EOF {
			s.Error = fmt.Errorf("string missing terminating \"'\"")
			return Error
		}
		if r == Error {
			return Error
		}
		if r == '\'' {
			break
		}
		if r == '\\' {
			r = s.readRune()
			if r == EOF {
				s.Error = fmt.Errorf("incomplete string escape")
				return Error
			}
			if r == Error {
				return Error
			}
		}
		s.buffer.WriteRune(r)
	}

	s.String = s.buffer.String()
	return String
}
