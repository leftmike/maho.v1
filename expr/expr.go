package expr

import (
	"fmt"

	"maho/sql"
)

type Op int

const (
	AddOp Op = iota
	AndOp
	BinaryAndOp
	BinaryOrOp
	ConcatOp
	DivideOp
	EqualOp
	GreaterEqualOp
	GreaterThanOp
	LessEqualOp
	LessThanOp
	LShiftOp
	ModuloOp
	MultiplyOp
	NegateOp
	NoOp
	NotEqualOp
	NotOp
	OrOp
	RShiftOp
	SubtractOp
)

var ops = [...]struct {
	name       string
	precedence int
}{
	AddOp:          {"+", 7},
	AndOp:          {"AND", 2},
	BinaryAndOp:    {"&", 6},
	BinaryOrOp:     {"|", 6},
	ConcatOp:       {"||", 10},
	DivideOp:       {"/", 8},
	EqualOp:        {"==", 4},
	GreaterEqualOp: {">=", 5},
	GreaterThanOp:  {">", 5},
	LessEqualOp:    {"<=", 5},
	LessThanOp:     {"<", 5},
	LShiftOp:       {"<<", 6},
	ModuloOp:       {"%", 8},
	MultiplyOp:     {"*", 8},
	NegateOp:       {"-", 9},
	NoOp:           {"", 11},
	NotEqualOp:     {"!=", 4},
	NotOp:          {"NOT", 3},
	OrOp:           {"OR", 1},
	RShiftOp:       {">>", 6},
	SubtractOp:     {"-", 7},
}

func (op Op) Precedence() int {
	return ops[op].precedence
}

func (op Op) String() string {
	return ops[op].name
}

type Expr interface {
	fmt.Stringer
}

type Literal struct {
	Value sql.Value
}

func (l *Literal) String() string {
	return sql.Format(l.Value)
}

type Unary struct {
	Op   Op
	Expr Expr
}

func (u *Unary) String() string {
	if ops[u.Op].name == "" {
		return u.Expr.String()
	}
	return fmt.Sprintf("(%s %s)", ops[u.Op].name, u.Expr)
}

type Binary struct {
	Op    Op
	Left  Expr
	Right Expr
}

func (b *Binary) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left, ops[b.Op].name, b.Right)
}

type Ref []sql.Identifier

func (r Ref) String() string {
	s := r[0].String()
	for i := 1; i < len(r); i++ {
		s += fmt.Sprintf(".%s", r[i])
	}
	return s
}

type Call struct {
	Name sql.Identifier
	Args []Expr
}

func (c *Call) String() string {
	s := fmt.Sprintf("%s(", c.Name)
	for i, a := range c.Args {
		if i > 0 {
			s += ", "
		}
		s += a.String()
	}
	s += ")"
	return s
}
