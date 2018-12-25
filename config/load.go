package config

import (
	"fmt"
	"io"
	"strings"
	"text/scanner"
	"unicode"
)

/*
BNF:
<config> = <definition> ...
<definition> = <name> '=' <value>
<value> = 'true' | 'false' | 'nil' | <string> | <number> | <map> | <array> | <identifier>
<map> = '{' <name> : <value> [ ',' ] ... [ ',' ] '}'
<array> = '[' <value> [ ',' ] ... [ ',' ] ']'
<string> = "..." | `...`
<number> = ...
<name> = <identifier> | <string>

Future:
<config> = <statement> ...
<statement> = <definition> | <section> | <assignment>
<section> = '[' <identifier> ']'
<statement> = ... | <assignment>
<assignment> = <variable> '=' <value>
<variable> = '$' '{' <identifier> '}'
<value> = ... | <variable>
*/

func scanArray(s *scanner.Scanner) (interface{}, error) {
	var a Array

	tok := s.Scan()
	for {
		if tok == ']' {
			break
		}
		v, err := scanValue(s, tok)
		if err != nil {
			return nil, err
		}
		a = append(a, v)
		tok = s.Scan()
		if tok == ',' {
			tok = s.Scan()
		}
	}

	return a, nil
}

func scanMap(s *scanner.Scanner) (interface{}, error) {
	m := Map{}

	tok := s.Scan()
	for {
		if tok == '}' {
			break
		}
		k := s.TokenText()
		if tok == scanner.String {
			k = strings.Trim(k, "\"`")
		} else if tok != scanner.Ident {
			return nil, fmt.Errorf("%s: expected an identifier or a string", s.Pos())
		}
		tok = s.Scan()
		if tok != ':' {
			return nil, fmt.Errorf("%s: expected ':'", s.Pos())
		}

		v, err := scanValue(s, s.Scan())
		if err != nil {
			return nil, err
		}
		m[k] = v
		tok = s.Scan()
		if tok == ',' {
			tok = s.Scan()
		}
	}

	return m, nil
}

func scanValue(s *scanner.Scanner, tok rune) (interface{}, error) {
	val := s.TokenText()
	switch tok {
	case scanner.Ident:
	case scanner.Int:
	case scanner.Float:
	case scanner.String:
		val = strings.Trim(val, `"`)
	case scanner.RawString:
		val = strings.Trim(val, "`")
	case '-':
		tok = s.Scan()
		if tok != scanner.Int && tok != scanner.Float {
			return nil, fmt.Errorf("%s: expected a number", s.Pos())
		}
		val = "-" + s.TokenText()
	case '[':
		return scanArray(s)
	case '{':
		return scanMap(s)
	default:
		return nil, fmt.Errorf("%s: expected a value", s.Pos())
	}
	return val, nil
}

func (c *Config) load(reader io.Reader) error {
	s := &scanner.Scanner{}
	s.Init(reader)
	s.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanStrings |
		scanner.ScanRawStrings | scanner.ScanComments | scanner.SkipComments
	s.IsIdentRune = func(r rune, i int) bool {
		if i == 0 {
			return unicode.IsLetter(r)
		}
		return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-'
	}

	for {
		npos := s.Pos()
		tok := s.Scan()
		if tok == scanner.EOF {
			break
		}

		name := s.TokenText()
		if tok == scanner.String {
			name = strings.Trim(name, "\"`")
		} else if tok != scanner.Ident {
			return fmt.Errorf("%s: expected an identifier or a string", npos)
		}

		tok = s.Scan()
		if tok != '=' {
			return fmt.Errorf("%s: expected '='", s.Pos())
		}

		cvar, ok := c.vars[name]
		if !ok {
			return fmt.Errorf("%s: %s is not a config variable", npos, name)
		}
		if cvar.noConfig {
			return fmt.Errorf("%s: %s can't be set in config file", npos, name)
		}

		vpos := s.Pos()
		val, err := scanValue(s, s.Scan())
		if err != nil {
			return err
		}

		if cvar.by == byDefault {
			err := cvar.val.SetValue(val)
			if err != nil {
				return fmt.Errorf("%s: %s: %s", vpos, cvar.name, err)
			}
			cvar.by = byConfig
		}
	}

	return nil
}
