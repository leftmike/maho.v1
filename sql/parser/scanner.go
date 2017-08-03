package parser

import (
	"bytes"
	"fmt"
	"io"
	"maho/sql"
	"maho/sql/token"
	"strconv"
	"unicode"
)

type position struct {
	filename string
	line     int
	column   int
}

type scanner struct {
	initialized bool
	rr          io.RuneReader
	unread      bool
	read        rune
	line        int
	column      int
	buffer      bytes.Buffer
	error       error
	identifier  sql.Identifier // Identifier and Reserved
	string      string
	integer     int64
	double      float64
	pos         position
}

func (pos position) String() string {
	s := pos.filename
	if pos.line > 0 {
		s += fmt.Sprintf(":%d:%d", pos.line, pos.column)
	}
	return s
}

func (s *scanner) Init(rr io.RuneReader, fn string) *scanner {
	if s.initialized {
		panic("scanner already initialized")
	}
	s.initialized = true

	s.rr = rr
	s.pos.filename = fn
	s.pos.line = 1
	s.pos.column = 0
	return s
}

func (s *scanner) Scan() rune {
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
			if r != token.EOF {
				return r
			}
		} else {
			s.unreadRune()
		}
	} else if r == '/' {
		if r := s.readRune(); r == '*' {
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
			if r != token.EOF {
				return r
			}
		} else {
			s.unreadRune()
		}
	}

	s.pos.column = s.column
	s.pos.line = s.line

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
	} else if token.IsOpRune(r) {
		s.buffer.WriteRune(r)
		r2 := s.readRune()
		if r2 == '-' || r2 == '+' {
			s.unreadRune()
			return r
		} else if token.IsOpRune(r2) {
			s.buffer.WriteRune(r2)
			if r, ok := token.Operators[s.buffer.String()]; ok {
				return r
			}
			s.error = fmt.Errorf("unexpected operator %s", s.buffer.String())
			return token.Error
		} else {
			s.unreadRune()
			return r
		}
	} else if r == '.' || r == ',' || r == '(' || r == ')' {
		return r
	}

	s.error = fmt.Errorf("unexpected character '%c'", r)
	return token.Error
}

func (s *scanner) readRune() rune {
	if s.unread {
		s.unread = false
		return s.read
	}

	var err error
	s.read, _, err = s.rr.ReadRune()
	if err == io.EOF {
		return token.EOF
	} else if err != nil {
		s.error = err
		return token.Error
	}

	if s.read == '\n' {
		s.line += 1
		s.column = 0
	} else {
		s.column += 1
	}

	return s.read
}

func (s *scanner) unreadRune() {
	s.unread = true
}

func (s *scanner) scanIdentifier(r rune) rune {
	for {
		s.buffer.WriteRune(r)
		r = s.readRune()
		if r == token.EOF {
			break
		} else if r == token.Error {
			return token.Error
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '$' {
			s.unreadRune()
			break
		}
	}

	s.identifier = sql.ID(s.buffer.String())
	if s.identifier.IsReserved() {
		return token.Reserved
	}
	return token.Identifier
}

func (s *scanner) scanNumber(r rune, sign int64) rune {
	dbl := false
	for {
		s.buffer.WriteRune(r)
		r = s.readRune()
		if r == token.EOF {
			break
		} else if r == token.Error {
			return token.Error
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
		s.double, err = strconv.ParseFloat(s.buffer.String(), 64)
	} else {
		s.integer, err = strconv.ParseInt(s.buffer.String(), 10, 64)
	}
	if err != nil {
		s.error = err
		return token.Error
	}
	if dbl {
		s.double *= float64(sign)
		return token.Double
	} else {
		s.integer *= sign
		return token.Integer
	}
}

func (s *scanner) scanQuotedIdentifier(delim rune) rune {
	for {
		r := s.readRune()
		if r == token.EOF {
			s.error = fmt.Errorf("quoted identifier missing terminating '%c'", delim)
			return token.Error
		}
		if r == token.Error {
			return token.Error
		}
		if r == delim {
			break
		}
		s.buffer.WriteRune(r)
	}

	s.identifier = sql.QuotedID(s.buffer.String())
	return token.Identifier
}

func (s *scanner) scanString() rune {
	for {
		r := s.readRune()
		if r == token.EOF {
			s.error = fmt.Errorf("string missing terminating \"'\"")
			return token.Error
		}
		if r == token.Error {
			return token.Error
		}
		if r == '\'' {
			break
		}
		if r == '\\' {
			r = s.readRune()
			if r == token.EOF {
				s.error = fmt.Errorf("incomplete string escape")
				return token.Error
			}
			if r == token.Error {
				return token.Error
			}
		}
		s.buffer.WriteRune(r)
	}

	s.string = s.buffer.String()
	return token.String
}
