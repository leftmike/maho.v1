package sql

import (
	"fmt"
)

type Expr interface {
	fmt.Stringer
	Equal(e Expr) bool
	HasRef() bool
}
