package expr

import (
	"fmt"

	"github.com/leftmike/maho/sql"
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
	Equal(e Expr) bool
	HasRef() bool
}

type Literal struct {
	Value sql.Value
}

func (l *Literal) String() string {
	return sql.Format(l.Value)
}

func (l *Literal) Equal(e Expr) bool {
	l2, ok := e.(*Literal)
	if !ok {
		return false
	}
	return sql.Compare(l.Value, l2.Value) == 0
}

func (_ *Literal) HasRef() bool {
	return false
}

func Nil() *Literal {
	return &Literal{nil}
}

func True() *Literal {
	return &Literal{sql.BoolValue(true)}
}

func False() *Literal {
	return &Literal{sql.BoolValue(false)}
}

func Int64Literal(i int64) *Literal {
	return &Literal{sql.Int64Value(i)}
}

func Float64Literal(f float64) *Literal {
	return &Literal{sql.Float64Value(f)}
}

func StringLiteral(s string) *Literal {
	return &Literal{sql.StringValue(s)}
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

func (u *Unary) Equal(e Expr) bool {
	u2, ok := e.(*Unary)
	if !ok {
		return false
	}
	return u.Op == u2.Op && u.Expr.Equal(u2.Expr)
}

func (u *Unary) HasRef() bool {
	return u.Expr.HasRef()
}

type Binary struct {
	Op    Op
	Left  Expr
	Right Expr
}

func (b *Binary) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left, ops[b.Op].name, b.Right)
}

func (b *Binary) Equal(e Expr) bool {
	b2, ok := e.(*Binary)
	if !ok {
		return false
	}
	return b.Op == b2.Op && b.Left.Equal(b2.Left) && b.Right.Equal(b2.Right)
}

func (b *Binary) HasRef() bool {
	return b.Left.HasRef() || b.Right.HasRef()
}

type Ref []sql.Identifier

func (r Ref) String() string {
	s := r[0].String()
	for i := 1; i < len(r); i++ {
		s += fmt.Sprintf(".%s", r[i])
	}
	return s
}

func (r Ref) Equal(e Expr) bool {
	r2, ok := e.(Ref)
	if !ok {
		return false
	}
	if len(r) != len(r2) {
		return false
	}
	for i := range r {
		if r[i] != r2[i] {
			return false
		}
	}
	return true
}

func (_ Ref) HasRef() bool {
	return true
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

func (c *Call) Equal(e Expr) bool {
	c2, ok := e.(*Call)
	if !ok {
		return false
	}
	if c.Name != c2.Name || len(c.Args) != len(c2.Args) {
		return false
	}
	for i := range c.Args {
		if !c.Args[i].Equal(c2.Args[i]) {
			return false
		}
	}
	return true
}

func (c *Call) HasRef() bool {
	for _, a := range c.Args {
		if a.HasRef() {
			return true
		}
	}
	return false
}
