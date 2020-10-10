package token

import (
	"fmt"
)

const (
	EOF = -(iota + 1)
	EndOfStatement
	Error
	Identifier
	Reserved
	String
	Bytes
	Integer
	Float
	Parameter

	BarBar
	LessLess
	LessEqual
	LessGreater
	GreaterGreater
	GreaterEqual
	EqualEqual
	BangEqual
)

const (
	Comma  = ','
	Dot    = '.'
	LParen = '('
	RParen = ')'
	AtSign = '@'
)

const (
	Minus     = '-'
	Plus      = '+'
	Star      = '*'
	Slash     = '/'
	Percent   = '%'
	Equal     = '='
	Less      = '<'
	Greater   = '>'
	Ampersand = '&'
	Bar       = '|'
	Bang      = '!'
)

var operators = map[rune]string{
	BarBar:         "||",
	LessLess:       "<<",
	LessEqual:      "<=",
	LessGreater:    "<>",
	GreaterGreater: ">>",
	GreaterEqual:   ">=",
	EqualEqual:     "==",
	BangEqual:      "!=",
}

var (
	opRunes = map[rune]bool{
		'-': true, '+': true, '*': true, '/': true, '%': true, '=': true, '<': true,
		'>': true, '&': true, '|': true, '!': true,
	}
	Operators = map[string]rune{}
)

func IsOpRune(r rune) bool {
	_, ok := opRunes[r]
	return ok
}

func Format(r rune) string {
	if r > 0 {
		return fmt.Sprintf("rune %c", r)
	}
	if s, ok := operators[r]; ok {
		return s
	}
	return fmt.Sprintf("token %d", r)
}

func init() {
	for r, s := range operators {
		Operators[s] = r
	}
}
