package expr

import (
	"fmt"
	"reflect"

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
	deepEqual(e Expr) bool
}

type Literal struct {
	Value sql.Value
}

func DeepEqual(e1, e2 Expr) bool {
	if e1 == nil || e2 == nil {
		return e1 == e2
	}
	return e1.deepEqual(e2)
}

func (l *Literal) String() string {
	return sql.Format(l.Value)
}

func (l *Literal) deepEqual(e Expr) bool {
	l2, ok := e.(*Literal)
	if !ok {
		return false
	}

	switch v := l.Value.(type) {
	case bool:
		if v2, ok := l2.Value.(bool); ok {
			return v == v2
		}
	case float64:
		if v2, ok := l2.Value.(float64); ok {
			return v == v2
		}
	case int64:
		if v2, ok := l2.Value.(int64); ok {
			return v == v2
		}
	case string:
		if v2, ok := l2.Value.(string); ok {
			return v == v2
		}
	default:
		panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", l.Value, l.Value))
	}
	return false
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

func (u *Unary) deepEqual(e Expr) bool {
	u2, ok := e.(*Unary)
	if !ok {
		return false
	}
	return u.Op == u2.Op && u.Expr.deepEqual(u2.Expr)
}

type Binary struct {
	Op    Op
	Left  Expr
	Right Expr
}

func (b *Binary) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left, ops[b.Op].name, b.Right)
}

func (b *Binary) deepEqual(e Expr) bool {
	b2, ok := e.(*Binary)
	if !ok {
		return false
	}
	return b.Op == b2.Op && b.Left.deepEqual(b2.Left) && b.Right.deepEqual(b2.Right)
}

type Ref []sql.Identifier

func (r Ref) String() string {
	s := r[0].String()
	for i := 1; i < len(r); i++ {
		s += fmt.Sprintf(".%s", r[i])
	}
	return s
}

func (r Ref) deepEqual(e Expr) bool {
	r2, ok := e.(Ref)
	if !ok {
		return false
	}
	return reflect.DeepEqual(r, r2)
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

func (c *Call) deepEqual(e Expr) bool {
	c2, ok := e.(*Call)
	if !ok {
		return false
	}
	if c.Name != c2.Name || len(c.Args) != len(c2.Args) {
		return false
	}

	for i := range c.Args {
		if !c.Args[i].deepEqual(c2.Args[i]) {
			return false
		}
	}
	return true
}
