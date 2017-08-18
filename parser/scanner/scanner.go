package scanner

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"unicode"

	"maho/parser/token"
	"maho/sql"
)

type Position struct {
	Filename string
	Line     int
	Column   int
}

type ScanCtx struct {
	Token      rune
	Error      error
	Identifier sql.Identifier // Identifier and Reserved
	String     string
	Integer    int64
	Double     float64
	Position
}

type Scanner struct {
	initialized bool
	rr          io.RuneReader
	unread      bool
	read        rune
	filename    string
	line        int
	column      int
	buffer      bytes.Buffer
}

func (pos Position) String() string {
	s := pos.Filename
	if pos.Line > 0 {
		s += fmt.Sprintf(":%d:%d", pos.Line, pos.Column)
	}
	return s
}

func (s *Scanner) Init(rr io.RuneReader, fn string) {
	if s.initialized {
		panic("scanner already initialized")
	}
	s.initialized = true

	s.rr = rr
	s.filename = fn
}

func (s *Scanner) Scan(sctx *ScanCtx) rune {
	s.buffer.Reset()
	sctx.Filename = s.filename
	sctx.Line = 1
	sctx.Column = 0
	sctx.Token = s.scan(sctx)
	return sctx.Token
}

func (s *Scanner) scan(sctx *ScanCtx) rune {
SkipWhitespace:
	r := s.readRune(sctx)

	for {
		if r < 0 {
			return r
		}
		if !unicode.IsSpace(r) {
			break
		}

		r = s.readRune(sctx)
	}

	if r == '-' {
		if r := s.readRune(sctx); r == '-' {
			for {
				r = s.readRune(sctx)
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
		if r := s.readRune(sctx); r == '*' {
			var p rune

			for {
				r = s.readRune(sctx)
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

	sctx.Column = s.column
	sctx.Line = s.line

	if unicode.IsLetter(r) || r == '_' {
		return s.scanIdentifier(sctx, r)
	} else if unicode.IsDigit(r) {
		return s.scanNumber(sctx, r, 1)
	} else if r == '+' {
		r = s.readRune(sctx)
		if unicode.IsDigit(r) {
			return s.scanNumber(sctx, r, 1)
		}
		s.unreadRune()
		return '+'
	} else if r == '-' {
		r = s.readRune(sctx)
		if unicode.IsDigit(r) {
			return s.scanNumber(sctx, r, -1)
		}
		s.unreadRune()
		return '-'
	} else if r == '"' || r == '`' {
		return s.scanQuotedIdentifier(sctx, r)
	} else if r == '[' {
		return s.scanQuotedIdentifier(sctx, ']')
	} else if r == '\'' {
		return s.scanString(sctx)
	} else if token.IsOpRune(r) {
		s.buffer.WriteRune(r)
		r2 := s.readRune(sctx)
		if r2 == '-' || r2 == '+' {
			s.unreadRune()
			return r
		} else if token.IsOpRune(r2) {
			s.buffer.WriteRune(r2)
			if r, ok := token.Operators[s.buffer.String()]; ok {
				return r
			}
			sctx.Error = fmt.Errorf("unexpected operator %s", s.buffer.String())
			return token.Error
		} else {
			s.unreadRune()
			return r
		}
	} else if r == '.' || r == ',' || r == '(' || r == ')' {
		return r
	}

	sctx.Error = fmt.Errorf("unexpected character '%c'", r)
	return token.Error
}

func (s *Scanner) readRune(sctx *ScanCtx) rune {
	if s.unread {
		s.unread = false
		return s.read
	}

	var err error
	s.read, _, err = s.rr.ReadRune()
	if err == io.EOF {
		return token.EOF
	} else if err != nil {
		sctx.Error = err
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

func (s *Scanner) unreadRune() {
	s.unread = true
}

func (s *Scanner) scanIdentifier(sctx *ScanCtx, r rune) rune {
	for {
		s.buffer.WriteRune(r)
		r = s.readRune(sctx)
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

	sctx.Identifier = sql.ID(s.buffer.String())
	if sctx.Identifier.IsReserved() {
		return token.Reserved
	}
	return token.Identifier
}

func (s *Scanner) scanNumber(sctx *ScanCtx, r rune, sign int64) rune {
	dbl := false
	for {
		s.buffer.WriteRune(r)
		r = s.readRune(sctx)
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
		sctx.Double, err = strconv.ParseFloat(s.buffer.String(), 64)
	} else {
		sctx.Integer, err = strconv.ParseInt(s.buffer.String(), 10, 64)
	}
	if err != nil {
		sctx.Error = err
		return token.Error
	}
	if dbl {
		sctx.Double *= float64(sign)
		return token.Double
	} else {
		sctx.Integer *= sign
		return token.Integer
	}
}

func (s *Scanner) scanQuotedIdentifier(sctx *ScanCtx, delim rune) rune {
	for {
		r := s.readRune(sctx)
		if r == token.EOF {
			sctx.Error = fmt.Errorf("quoted identifier missing terminating '%c'", delim)
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

	sctx.Identifier = sql.QuotedID(s.buffer.String())
	return token.Identifier
}

func (s *Scanner) scanString(sctx *ScanCtx) rune {
	for {
		r := s.readRune(sctx)
		if r == token.EOF {
			sctx.Error = fmt.Errorf("string missing terminating \"'\"")
			return token.Error
		}
		if r == token.Error {
			return token.Error
		}
		if r == '\'' {
			break
		}
		if r == '\\' {
			r = s.readRune(sctx)
			if r == token.EOF {
				sctx.Error = fmt.Errorf("incomplete string escape")
				return token.Error
			}
			if r == token.Error {
				return token.Error
			}
		}
		s.buffer.WriteRune(r)
	}

	sctx.String = s.buffer.String()
	return token.String
}
