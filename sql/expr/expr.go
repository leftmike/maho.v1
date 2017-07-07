package expr

import (
	"fmt"
	"maho/sql"
)

type Op int

const (
	NoOp Op = iota
	NegateOp
	AddOp
	SubtractOp
	MultiplyOp
	DivideOp
	EqualOp
)

var ops = [...]struct {
	name       string
	precedence int
}{
	NoOp:       {name: ""},
	NegateOp:   {name: "-"},
	EqualOp:    {"=", 1},
	AddOp:      {"+", 2},
	SubtractOp: {"-", 2},
	MultiplyOp: {"*", 3},
	DivideOp:   {"/", 3},
}

func (op Op) Precedence() int {
	return ops[op].precedence
}

type EvalCtx interface {
}

type Expr interface {
	fmt.Stringer
	Eval(ctx EvalCtx) (interface{}, error)
}

type Literal struct {
	Value sql.Value
}

func (l *Literal) String() string {
	return sql.FormatValue(l.Value)
}

func (l *Literal) Eval(ctx EvalCtx) (interface{}, error) {
	return l.Value, nil
}

type Unary struct {
	Op   Op
	Expr Expr
}

func (u *Unary) String() string {
	if ops[u.Op].name == "" {
		return u.Expr.String()
	}
	return fmt.Sprintf("%s %s", ops[u.Op].name, u.Expr)
}

func (u *Unary) Eval(ctx EvalCtx) (interface{}, error) {
	return u.Expr.Eval(ctx) // XXX: fix this
}

type Binary struct {
	Op    Op
	Left  Expr
	Right Expr
}

func (b *Binary) String() string {
	// XXX: () not always necessary
	return fmt.Sprintf("(%s %s %s)", b.Left, ops[b.Op].name, b.Right)
}

func (b *Binary) Eval(ctx EvalCtx) (interface{}, error) {
	return b.Left.Eval(ctx) // XXX: fix this
}

type Variable struct {
	name     sql.Identifier
	table    sql.Identifier
	database sql.Identifier
}

type Call struct {
	name sql.Identifier
	args []Expr
}
