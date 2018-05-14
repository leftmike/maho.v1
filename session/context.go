package session

import (
	"github.com/leftmike/maho/sql"
)

type Context interface {
	DefaultEngine() string
	DefaultDatabase() sql.Identifier
}

type context struct {
	eng  string
	name sql.Identifier
}

func NewContext(eng string, name sql.Identifier) Context {
	return &context{
		eng:  eng,
		name: name,
	}
}

func (ctx *context) DefaultEngine() string {
	return ctx.eng
}

func (ctx *context) DefaultDatabase() sql.Identifier {
	return ctx.name
}
