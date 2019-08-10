package scanner

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"unicode"

	"github.com/leftmike/maho/parser/token"
	"github.com/leftmike/maho/sql"
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
	Float      float64
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

func (s *Scanner) Scan(sctx *ScanCtx) {
	s.buffer.Reset()
	sctx.Filename = s.filename
	sctx.Line = 1
	sctx.Column = 0
	sctx.Token = s.scan(sctx)
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

	if r == ';' {
		return token.EndOfStatement
	}

	if r == '-' {
		if r2 := s.readRune(sctx); r2 == '-' {
			for {
				r2 = s.readRune(sctx)
				if r2 < 0 {
					return r2
				}

				if r2 == '\n' {
					break
				}
			}

			goto SkipWhitespace
		} else if r2 < 0 {
			return r2
		} else {
			s.unreadRune()
		}
	} else if r == '/' {
		if r2 := s.readRune(sctx); r2 == '*' {
			var p rune

			for {
				r2 = s.readRune(sctx)
				if r2 < 0 {
					return r2
				}

				if p == '*' && r2 == '/' {
					break
				}
				p = r2
			}

			goto SkipWhitespace
		} else if r2 < 0 {
			return r2
		} else {
			s.unreadRune()
		}
	}

	sctx.Column = s.column
	sctx.Line = s.line

	if r == 'e' || r == 'E' {
		if s.readRune(sctx) == '\'' {
			return s.scanString(sctx, true)
		} else {
			s.unreadRune()
			return s.scanIdentifier(sctx, r)
		}
	} else if unicode.IsLetter(r) || r == '_' {
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
		return s.scanString(sctx, false)
	} else if token.IsOpRune(r) {
		s.buffer.WriteRune(r)
		r2 := s.readRune(sctx)
		if r2 == '-' || r2 == '+' {
			s.unreadRune()
			return r
		} else if token.IsOpRune(r2) {
			s.buffer.WriteRune(r2)
			if r3, ok := token.Operators[s.buffer.String()]; ok {
				return r3
			}
			sctx.Error = fmt.Errorf("scanner: unexpected operator %s", s.buffer.String())
			return token.Error
		} else {
			s.unreadRune()
			return r
		}
	} else if r == '.' || r == ',' || r == '(' || r == ')' {
		return r
	}

	sctx.Error = fmt.Errorf("scanner: unexpected character '%c'", r)
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
		s.read = token.EOF
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

	sctx.Identifier = sql.UnquotedID(s.buffer.String())
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
		sctx.Float, err = strconv.ParseFloat(s.buffer.String(), 64)
	} else {
		sctx.Integer, err = strconv.ParseInt(s.buffer.String(), 10, 64)
	}
	if err != nil {
		sctx.Error = err
		return token.Error
	}
	if dbl {
		sctx.Float *= float64(sign)
		return token.Float
	} else {
		sctx.Integer *= sign
		return token.Integer
	}
}

func (s *Scanner) scanQuotedIdentifier(sctx *ScanCtx, delim rune) rune {
	for {
		r := s.readRune(sctx)
		if r == token.EOF {
			sctx.Error = fmt.Errorf("scanner: quoted identifier missing terminating '%c'", delim)
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

func (s *Scanner) scanHexDigit(sctx *ScanCtx) (uint, bool) {
	r := s.readRune(sctx)
	if r >= '0' && r <= '9' {
		return uint(r - '0'), true
	} else if r >= 'A' && r <= 'F' {
		return uint(r - 'A' + 10), true
	} else if r >= 'a' && r <= 'f' {
		return uint(r - 'a' + 10), true
	}
	if r != token.Error {
		sctx.Error = fmt.Errorf("scanner: expected hex digit")
	}
	return 0, false
}

func (s *Scanner) scanHex(sctx *ScanCtx) rune {
	hex, ok := s.scanHexDigit(sctx)
	if !ok {
		return token.Error
	}
	d, ok := s.scanHexDigit(sctx)
	if !ok {
		return token.Error
	}
	return rune(hex*16 + d)
}

func (s *Scanner) scanUnicode(sctx *ScanCtx, digits int) rune {
	u := uint(0)
	for digits > 0 {
		d, ok := s.scanHexDigit(sctx)
		if !ok {
			return token.Error
		}
		u = u*16 + d
		digits -= 1
	}
	return rune(u)
}

func (s *Scanner) scanOctalDigit(sctx *ScanCtx) (byte, bool) {
	r := s.readRune(sctx)
	if r >= '0' && r <= '7' {
		return byte(r - '0'), true
	}
	if r != token.Error {
		sctx.Error = fmt.Errorf("scanner: expected octal digit")
	}
	return 0, false
}

func (s *Scanner) scanOctal(sctx *ScanCtx, r rune) rune {
	octal := byte(r - '0')
	d, ok := s.scanOctalDigit(sctx)
	if !ok {
		return token.Error
	}
	octal = octal*8 + d
	d, ok = s.scanOctalDigit(sctx)
	if !ok {
		return token.Error
	}
	return rune(octal*8 + d)
}

func (s *Scanner) scanString(sctx *ScanCtx, esc bool) rune {
	for {
		r := s.readRune(sctx)
		if r == token.EOF {
			sctx.Error = fmt.Errorf("scanner: string missing terminating \"'\"")
			return token.Error
		}
		if r == token.Error {
			return token.Error
		}
		if r == '\'' {
			r = s.readRune(sctx)
			if unicode.IsSpace(r) {
				var nl bool
				for unicode.IsSpace(r) {
					if r == 10 || r == 13 {
						nl = true
					}
					r = s.readRune(sctx)
				}
				if r == '\'' && nl {
					// Concatenate strings only separated by whitespace with at least one newline.
					continue
				} else if r == token.Error {
					return token.Error
				} else {
					s.unreadRune()
					break
				}
			} else if r != '\'' {
				s.unreadRune()
				break
			}
		}
		if r == '\\' && esc {
			r = s.readRune(sctx)
			switch r {
			case 'a':
				r = 7 // bell
			case 'b':
				r = 8 // backspace
			case 't':
				r = 9 // tab
			case 'n':
				r = 10 // newline
			case 'v':
				r = 11 // vertical tab
			case 'f':
				r = 12 // form feed
			case 'r':
				r = 13 // carriage return
			case 'x':
				r = s.scanHex(sctx)
			case 'u':
				r = s.scanUnicode(sctx, 4)
			case 'U':
				r = s.scanUnicode(sctx, 8)
			case '0', '1', '2', '3', '4', '5', '6', '7':
				r = s.scanOctal(sctx, r)
			}

			if r == token.EOF {
				sctx.Error = fmt.Errorf("scanner: incomplete string escape")
				return token.Error
			} else if r == token.Error {
				return token.Error
			}
		}
		s.buffer.WriteRune(r)
	}

	sctx.String = s.buffer.String()
	return token.String
}
