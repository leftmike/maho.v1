package session

import (
	"context"

	"github.com/leftmike/maho/sql"
)

type Context interface {
	Context() context.Context
	DefaultEngine() string
	DefaultDatabase() sql.Identifier
}

type sess struct {
	eng  string
	name sql.Identifier
}

func NewContext(eng string, name sql.Identifier) Context {
	return &sess{
		eng:  eng,
		name: name,
	}
}

func (ctx *sess) Context() context.Context {
	return nil
}

func (ctx *sess) DefaultEngine() string {
	return ctx.eng
}

func (ctx *sess) DefaultDatabase() sql.Identifier {
	return ctx.name
}
